use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};
use crate::indexes::{IndexManager, IndexType};
use crate::indexes::index_manager::{IndexUpdate, IndexOperationType};
use crate::error::ReefDBError;
use crate::fts::search::Search;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OnDiskIndexManager {
    file_path: String,
    indexes: HashMap<String, HashMap<String, IndexType>>,
    #[serde(skip)]
    pending_updates: HashMap<u64, Vec<IndexUpdate>>,
    #[serde(skip)]
    active_transactions: HashSet<u64>,
    #[serde(skip)]
    undo_log: HashMap<u64, Vec<IndexUpdate>>,
}

impl OnDiskIndexManager {
    pub fn new(file_path: String) -> Self {
        let path = Path::new(&file_path);
        let mut indexes = HashMap::new();
        if path.exists() {
            let mut file = File::open(path).unwrap();
            let mut buffer = Vec::new();
            if file.read_to_end(&mut buffer).unwrap() > 0 {
                indexes = deserialize(&buffer).unwrap();
            }
        }
        OnDiskIndexManager {
            file_path,
            indexes,
            pending_updates: HashMap::new(),
            active_transactions: HashSet::new(),
            undo_log: HashMap::new(),
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = Path::new(&self.file_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)?;
        let mut writer = BufWriter::new(file);
        let buffer = serialize(&self.indexes).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writer.write_all(&buffer)?;
        writer.flush()?;
        writer.get_ref().sync_all()
    }

    fn get_index_internal(&self, table: &str, column: &str) -> Option<&IndexType> {
        println!("Getting index for table: {}, column: {}", table, column);
        println!("Available indexes: {:?}", self.indexes.keys().collect::<Vec<_>>());
        println!("Table indexes: {:?}", self.indexes.get(table).map(|t| t.keys().collect::<Vec<_>>()));
        self.indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(column))
    }

    fn apply_update(&mut self, update: &IndexUpdate) -> Result<(), ReefDBError> {
        match update.operation_type {
            IndexOperationType::Insert => {
                if let Some(new_value) = &update.new_value {
                    self.update_index(
                        &update.table_name,
                        &update.column_name,
                        Vec::new(),
                        new_value.clone(),
                        update.row_id,
                    )?;
                }
            }
            IndexOperationType::Update => {
                if let (Some(old_value), Some(new_value)) = (&update.old_value, &update.new_value) {
                    self.update_index(
                        &update.table_name,
                        &update.column_name,
                        old_value.clone(),
                        new_value.clone(),
                        update.row_id,
                    )?;
                }
            }
            IndexOperationType::Delete => {
                if let Some(old_value) = &update.old_value {
                    self.update_index(
                        &update.table_name,
                        &update.column_name,
                        old_value.clone(),
                        Vec::new(),
                        update.row_id,
                    )?;
                }
            }
        }
        self.save()?;
        Ok(())
    }

    fn record_undo(&mut self, update: IndexUpdate) {
        self.undo_log
            .entry(update.transaction_id)
            .or_insert_with(Vec::new)
            .push(update);
    }
}

impl IndexManager for OnDiskIndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) -> Result<(), ReefDBError> {
        let table_indexes = self.indexes.entry(table.to_string()).or_insert_with(HashMap::new);
        table_indexes.insert(column.to_string(), index_type);
        self.save()?;
        Ok(())
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            table_indexes.remove(column);
            let _ = self.save();
        }
    }

    fn get_index(&self, table: &str, column: &str) -> Result<&IndexType, ReefDBError> {
        println!("Getting index for table: {}, column: {}", table, column);
        println!("Available indexes: {:?}", self.indexes.keys().collect::<Vec<_>>());
        let table_indexes = self.indexes.get(table)
            .ok_or_else(|| ReefDBError::TableNotFound(table.to_string()))?;
        println!("Table indexes: {:?}", Some(table_indexes.keys().collect::<Vec<_>>()));
        table_indexes.get(column)
            .ok_or_else(|| ReefDBError::ColumnNotFound(column.to_string()))
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) -> Result<(), ReefDBError> {
        println!("Updating index for table: {}, column: {}", table, column);
        println!("old_value: {:?}, new_value: {:?}, row_id: {}", old_value, new_value, row_id);
        
        let table_indexes = self.indexes.get_mut(table)
            .ok_or_else(|| ReefDBError::TableNotFound(table.to_string()))?;
        
        let index = table_indexes.get_mut(column)
            .ok_or_else(|| ReefDBError::ColumnNotFound(column.to_string()))?;

        match index {
            IndexType::BTree(btree) => {
                if !old_value.is_empty() {
                    btree.remove_entry(old_value.clone(), row_id);
                }
                btree.add_entry(new_value, row_id);
            }
            IndexType::GIN(gin) => {
                if !old_value.is_empty() {
                    gin.remove_document(table, column, row_id);
                }
                gin.add_document(table, column, row_id, std::str::from_utf8(&new_value).unwrap_or_default());
            }
        }
        self.save()?;
        Ok(())
    }

    fn track_index_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError> {
        // Record the transaction as active
        self.active_transactions.insert(update.transaction_id);

        // Create an undo record before applying the update
        let undo_update = IndexUpdate {
            table_name: update.table_name.clone(),
            column_name: update.column_name.clone(),
            old_value: update.new_value.clone(),
            new_value: update.old_value.clone(),
            row_id: update.row_id,
            transaction_id: update.transaction_id,
            operation_type: match update.operation_type {
                IndexOperationType::Insert => IndexOperationType::Delete,
                IndexOperationType::Delete => IndexOperationType::Insert,
                IndexOperationType::Update => IndexOperationType::Update,
            },
        };
        self.record_undo(undo_update);

        // Store the update in pending updates
        self.pending_updates
            .entry(update.transaction_id)
            .or_insert_with(Vec::new)
            .push(update.clone());

        // Apply the update and persist to disk
        self.apply_update(&update)
    }

    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        // Remove transaction data
        self.pending_updates.remove(&transaction_id);
        self.undo_log.remove(&transaction_id);
        self.active_transactions.remove(&transaction_id);
        self.save()?;
        Ok(())
    }

    fn rollback_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if let Some(undo_records) = self.undo_log.remove(&transaction_id) {
            // Apply undo records in reverse order
            for undo_update in undo_records.into_iter().rev() {
                self.apply_update(&undo_update)?;
            }
        }

        // Clean up transaction data
        self.pending_updates.remove(&transaction_id);
        self.active_transactions.remove(&transaction_id);
        self.save()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use crate::indexes::btree::BTreeIndex;

    #[test]
    fn test_btree_index() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree.idx");
        let mut manager = OnDiskIndexManager::new(file_path.to_str().unwrap().to_string());

        // Create a BTree index
        let mut btree = BTreeIndex::new();
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 2);
        
        manager.create_index("users", "age", IndexType::BTree(btree)).unwrap();
        
        // Test searching
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            assert!(index.search(vec![1, 2, 3]).unwrap().contains(&1));
            assert!(index.search(vec![4, 5, 6]).unwrap().contains(&2));
        } else {
            panic!("Failed to get index");
        }
    }

    #[test]
    fn test_transaction_commit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_commit.idx");
        let mut manager = OnDiskIndexManager::new(file_path.to_str().unwrap().to_string());

        // Add initial data
        let mut btree = BTreeIndex::new();
        btree.add_entry(vec![1, 2, 3], 1);
        manager.create_index("users", "age", IndexType::BTree(btree)).unwrap();

        let transaction_id = 1;
        let update = IndexUpdate {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![7, 8, 9]),
            row_id: 1,
            transaction_id,
            operation_type: IndexOperationType::Update,
        };

        // Track and apply the update
        manager.track_index_update(update).unwrap();
        
        // Verify the update is immediately visible
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9]");
            let old_result = index.search(vec![1, 2, 3]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3]");
        } else {
            panic!("Failed to get index");
        }

        // Commit the transaction
        manager.commit_index_transaction(transaction_id).unwrap();
        
        // Verify update remains visible after commit
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9] after commit");
            let old_result = index.search(vec![1, 2, 3]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3] after commit");
        } else {
            panic!("Failed to get index");
        }
    }
} 