use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::fmt::Debug;
use std::io::{Read, Write, BufWriter};
use std::fs::{File, OpenOptions};
use std::path::Path;
use serde::{Serialize, Deserialize};
use crate::fts::default::DefaultSearchIdx;
use crate::fts::tokenizers::tokenizer::Tokenizer;
use crate::fts::tokenizers::default::DefaultTokenizer;
use crate::indexes::gin::GinIndex;
use crate::indexes::btree::BTreeIndex;
use crate::fts::search::Search;
use crate::error::ReefDBError;

#[derive(Debug, Serialize, Deserialize)]
pub enum IndexType {
    BTree(BTreeIndex),
    GIN(GinIndex<DefaultTokenizer>),
}

impl Clone for IndexType {
    fn clone(&self) -> Self {
        match self {
            IndexType::BTree(btree) => IndexType::BTree(btree.clone()),
            IndexType::GIN(gin) => IndexType::GIN(gin.clone()),
        }
    }
}

pub trait IndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) -> Result<(), ReefDBError>;
    fn drop_index(&mut self, table: &str, column: &str);
    fn get_index(&self, table: &str, column: &str) -> Result<&IndexType, ReefDBError>;
    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) -> Result<(), ReefDBError>;
    
    // Transaction-aware methods
    fn track_index_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError>;
    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError>;
    fn rollback_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUpdate {
    pub table_name: String,
    pub column_name: String,
    pub old_value: Option<Vec<u8>>,
    pub new_value: Option<Vec<u8>>,
    pub row_id: usize,
    pub transaction_id: u64,
    pub operation_type: IndexOperationType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexOperationType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultIndexManager {
    indexes: HashMap<String, HashMap<String, IndexType>>,
    pending_updates: HashMap<u64, Vec<IndexUpdate>>,
    active_transactions: HashSet<u64>,
    undo_log: HashMap<u64, Vec<IndexUpdate>>,
}

impl DefaultIndexManager {
    pub fn new() -> DefaultIndexManager {
        DefaultIndexManager {
            indexes: HashMap::new(),
            pending_updates: HashMap::new(),
            active_transactions: HashSet::new(),
            undo_log: HashMap::new(),
        }
    }

    fn get_index_internal(&self, table: &str, column: &str) -> Option<&IndexType> {
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
        Ok(())
    }

    fn record_undo(&mut self, update: IndexUpdate) {
        self.undo_log
            .entry(update.transaction_id)
            .or_insert_with(Vec::new)
            .push(update);
    }
}

impl IndexManager for DefaultIndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) -> Result<(), ReefDBError> {
        self.indexes
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), index_type);
        Ok(())
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            table_indexes.remove(column);
            if table_indexes.is_empty() {
                self.indexes.remove(table);
            }
        }
    }

    fn get_index(&self, table: &str, column: &str) -> Result<&IndexType, ReefDBError> {
        self.indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(column))
            .ok_or_else(|| ReefDBError::Other(format!("Index not found for {}.{}", table, column)))
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) -> Result<(), ReefDBError> {
        if let Some(index) = self.indexes
            .get_mut(table)
            .and_then(|table_indexes| table_indexes.get_mut(column)) {
            match index {
                IndexType::BTree(btree) => {
                    btree.remove_entry(old_value, row_id);
                    btree.add_entry(new_value, row_id);
                }
                IndexType::GIN(gin) => {
                    // For GIN indexes, we need to handle text differently
                    let old_text = String::from_utf8_lossy(&old_value).to_string();
                    let new_text = String::from_utf8_lossy(&new_value).to_string();
                    gin.update_document(table, column, row_id, &old_text);
                    gin.add_document(table, column, row_id, &new_text);
                }
            }
            Ok(())
        } else {
            Err(ReefDBError::Other(format!("Index not found for {}.{}", table, column)))
        }
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

        // Apply the update
        self.apply_update(&update)
    }

    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        // Remove transaction data
        self.pending_updates.remove(&transaction_id);
        self.undo_log.remove(&transaction_id);
        self.active_transactions.remove(&transaction_id);
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
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskIndexManager {
    indexes: HashMap<String, HashMap<String, IndexType>>,
    pending_updates: HashMap<u64, Vec<IndexUpdate>>,
    active_transactions: HashSet<u64>,
    index_path: String,
}

impl OnDiskIndexManager {
    pub fn new(index_path: String) -> Self {
        let mut indexes = HashMap::new();
        let path = std::path::Path::new(&index_path);
        if path.exists() {
            let mut file = std::fs::File::open(path).unwrap();
            let mut buffer = Vec::new();
            if file.read_to_end(&mut buffer).unwrap() > 0 {
                indexes = bincode::deserialize(&buffer).unwrap();
            }
        }

        OnDiskIndexManager {
            indexes,
            pending_updates: HashMap::new(),
            active_transactions: HashSet::new(),
            index_path,
        }
    }

    fn save(&self) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.index_path)?;
        let mut writer = std::io::BufWriter::new(file);
        let serialized = bincode::serialize(&self.indexes).unwrap();
        writer.write_all(&serialized)?;
        writer.flush()?;
        Ok(())
    }

    fn get_index_internal(&self, table: &str, column: &str) -> Option<&IndexType> {
        self.indexes.get(table).and_then(|cols| cols.get(column))
    }

    fn get_index_internal_mut(&mut self, table: &str, column: &str) -> Option<&mut IndexType> {
        self.indexes.get_mut(table).and_then(|cols| cols.get_mut(column))
    }

    fn apply_update(&mut self, update: &IndexUpdate) -> Result<(), ReefDBError> {
        match update.operation_type {
            IndexOperationType::Insert => {
                if let Some(new_value) = &update.new_value {
                    match self.get_index_internal_mut(&update.table_name, &update.column_name) {
                        Some(IndexType::BTree(btree)) => {
                            btree.add_entry(new_value.clone(), update.row_id);
                        },
                        Some(IndexType::GIN(gin)) => {
                            gin.add_document(&update.table_name, &update.column_name, update.row_id, std::str::from_utf8(new_value).unwrap_or_default());
                        },
                        None => return Err(ReefDBError::Other("Index not found".to_string())),
                    }
                }
            },
            IndexOperationType::Update => {
                if let (Some(old_value), Some(new_value)) = (&update.old_value, &update.new_value) {
                    match self.get_index_internal_mut(&update.table_name, &update.column_name) {
                        Some(IndexType::BTree(btree)) => {
                            btree.remove_entry(old_value.clone(), update.row_id);
                            btree.add_entry(new_value.clone(), update.row_id);
                        },
                        Some(IndexType::GIN(gin)) => {
                            gin.remove_document(&update.table_name, &update.column_name, update.row_id);
                            gin.add_document(&update.table_name, &update.column_name, update.row_id, std::str::from_utf8(new_value).unwrap_or_default());
                        },
                        None => return Err(ReefDBError::Other("Index not found".to_string())),
                    }
                }
            },
            IndexOperationType::Delete => {
                if let Some(old_value) = &update.old_value {
                    match self.get_index_internal_mut(&update.table_name, &update.column_name) {
                        Some(IndexType::BTree(btree)) => {
                            btree.remove_entry(old_value.clone(), update.row_id);
                        },
                        Some(IndexType::GIN(gin)) => {
                            gin.remove_document(&update.table_name, &update.column_name, update.row_id);
                        },
                        None => return Err(ReefDBError::Other("Index not found".to_string())),
                    }
                }
            }
        }
        Ok(())
    }

    fn write_wal_entry(&self, _entry: &IndexWALEntry) -> Result<(), ReefDBError> {
        // No-op: index WAL is disabled in favor of unified command logging
        Ok(())
    }

    fn recover_from_wal(&mut self) -> Result<(), ReefDBError> {
        // No-op: index WAL is disabled. Remove any stale WAL file for cleanliness.
        let wal_path = format!("{}.wal", self.index_path);
        if Path::new(&wal_path).exists() {
            let _ = std::fs::remove_file(wal_path);
        }
        Ok(())
    }
}

impl IndexManager for OnDiskIndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) -> Result<(), ReefDBError> {
        self.indexes
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), index_type);
        self.save().map_err(|e| ReefDBError::IoError(e.to_string()))?;
        Ok(())
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            table_indexes.remove(column);
            if table_indexes.is_empty() {
                self.indexes.remove(table);
            }
            self.save().unwrap();
        }
    }

    fn get_index(&self, table: &str, column: &str) -> Result<&IndexType, ReefDBError> {
        self.indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(column))
            .ok_or_else(|| ReefDBError::Other(format!("Index not found for {}.{}", table, column)))
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) -> Result<(), ReefDBError> {
        if let Some(index) = self.indexes
            .get_mut(table)
            .and_then(|table_indexes| table_indexes.get_mut(column)) {
            match index {
                IndexType::BTree(btree) => {
                    btree.remove_entry(old_value, row_id);
                    btree.add_entry(new_value, row_id);
                }
                IndexType::GIN(gin) => {
                    // For GIN indexes, we need to handle text differently
                    let old_text = String::from_utf8_lossy(&old_value).to_string();
                    let new_text = String::from_utf8_lossy(&new_value).to_string();
                    gin.update_document(table, column, row_id, &old_text);
                    gin.add_document(table, column, row_id, &new_text);
                }
            }
            self.save().map_err(|e| ReefDBError::IoError(e.to_string()))?;
            Ok(())
        } else {
            Err(ReefDBError::Other(format!("Index not found for {}.{}", table, column)))
        }
    }

    fn track_index_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError> {
        let wal_entry = IndexWALEntry {
            transaction_id: update.transaction_id,
            operation: IndexWALOperation::Update(update.clone()),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        self.write_wal_entry(&wal_entry)?;
        
        if let Some(updates) = self.pending_updates.get_mut(&update.transaction_id) {
            updates.push(update.clone());
        } else {
            let begin_entry = IndexWALEntry {
                transaction_id: update.transaction_id,
                operation: IndexWALOperation::Begin,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };
            self.write_wal_entry(&begin_entry)?;
            self.pending_updates.insert(update.transaction_id, vec![update.clone()]);
        }
        self.active_transactions.insert(update.transaction_id);
        Ok(())
    }

    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        let commit_entry = IndexWALEntry {
            transaction_id,
            operation: IndexWALOperation::Commit,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        self.write_wal_entry(&commit_entry)?;

        if let Some(updates) = self.pending_updates.remove(&transaction_id) {
            for update in updates {
                self.apply_update(&update)?;
            }
        }
        self.active_transactions.remove(&transaction_id);
        self.save()?;
        Ok(())
    }

    fn rollback_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        let rollback_entry = IndexWALEntry {
            transaction_id,
            operation: IndexWALOperation::Rollback,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        self.write_wal_entry(&rollback_entry)?;

        self.pending_updates.remove(&transaction_id);
        self.active_transactions.remove(&transaction_id);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexWALEntry {
    pub transaction_id: u64,
    pub operation: IndexWALOperation,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexWALOperation {
    Begin,
    Update(IndexUpdate),
    Commit,
    Rollback,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_btree_index() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add some entries
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 2);
        
        // Create index
        manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
        
        // Verify index was created
        let index = manager.get_index("test_table", "test_column").unwrap();
        match index {
            IndexType::BTree(_) => (),
            _ => panic!("Expected BTree index"),
        }
    }

    #[test]
    fn test_transaction_commit() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add initial entry
        btree.add_entry(vec![1, 2, 3], 1);
        manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
        
        // Create transaction update
        let update = IndexUpdate {
            table_name: "test_table".to_string(),
            column_name: "test_column".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![4, 5, 6]),
            row_id: 1,
            transaction_id: 1,
            operation_type: IndexOperationType::Insert,
        };
        
        // Track and commit update
        manager.track_index_update(update).unwrap();
        manager.commit_index_transaction(1).unwrap();
    }

    #[test]
    fn test_transaction_rollback() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add initial entry
        btree.add_entry(vec![1, 2, 3], 1);
        manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
        
        // Create transaction update
        let update = IndexUpdate {
            table_name: "test_table".to_string(),
            column_name: "test_column".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![4, 5, 6]),
            row_id: 1,
            transaction_id: 1,
            operation_type: IndexOperationType::Insert,
        };
        
        // Track and rollback update
        manager.track_index_update(update).unwrap();
        manager.rollback_index_transaction(1).unwrap();
    }

    #[test]
    fn test_concurrent_transactions() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add initial entries
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 2);
        manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
        
        // Create transaction 1 update
        let update1 = IndexUpdate {
            table_name: "test_table".to_string(),
            column_name: "test_column".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![7, 8, 9]),
            row_id: 1,
            transaction_id: 1,
            operation_type: IndexOperationType::Insert,
        };
        
        // Create transaction 2 update
        let update2 = IndexUpdate {
            table_name: "test_table".to_string(),
            column_name: "test_column".to_string(),
            old_value: Some(vec![4, 5, 6]),
            new_value: Some(vec![10, 11, 12]),
            row_id: 2,
            transaction_id: 2,
            operation_type: IndexOperationType::Insert,
        };
        
        // Track both updates
        manager.track_index_update(update1).unwrap();
        manager.track_index_update(update2).unwrap();
        
        // Commit transaction 1, rollback transaction 2
        manager.commit_index_transaction(1).unwrap();
        manager.rollback_index_transaction(2).unwrap();
    }

    #[test]
    fn test_index_recovery() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test_index").to_str().unwrap().to_string();
        
        // Create initial manager and add some data
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            let mut btree = BTreeIndex::new();
            btree.add_entry(vec![1, 2, 3], 1);
            manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
            
            // Start a transaction
            let update = IndexUpdate {
                table_name: "test_table".to_string(),
                column_name: "test_column".to_string(),
                old_value: None,
                new_value: Some(vec![4, 5, 6]),
                row_id: 2,
                transaction_id: 1,
                operation_type: IndexOperationType::Insert,
            };
            
            manager.track_index_update(update).unwrap();
            // Don't commit - simulate crash
        }

        // Create new manager instance to test recovery
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            manager.recover_from_wal().unwrap();
            
            // Verify the uncommitted transaction was rolled back
            let index = manager.get_index("test_table", "test_column").unwrap();
            match index {
                IndexType::BTree(btree) => {
                    assert!(btree.search(vec![1, 2, 3]).is_some()); // Initial entry exists
                    assert!(btree.search(vec![4, 5, 6]).is_none()); // Uncommitted entry doesn't exist
                },
                _ => panic!("Expected BTree index"),
            }
        }
    }

    #[test]
    fn test_index_recovery_with_multiple_transactions() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test_index").to_str().unwrap().to_string();
        
        // Create initial manager and add some data
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            let mut btree = BTreeIndex::new();
            btree.add_entry(vec![1, 2, 3], 1);
            manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
            
            // Transaction 1 - will be committed
            let update1 = IndexUpdate {
                table_name: "test_table".to_string(),
                column_name: "test_column".to_string(),
                old_value: None,
                new_value: Some(vec![4, 5, 6]),
                row_id: 2,
                transaction_id: 1,
                operation_type: IndexOperationType::Insert,
            };
            
            // Transaction 2 - will be uncommitted
            let update2 = IndexUpdate {
                table_name: "test_table".to_string(),
                column_name: "test_column".to_string(),
                old_value: None,
                new_value: Some(vec![7, 8, 9]),
                row_id: 3,
                transaction_id: 2,
                operation_type: IndexOperationType::Insert,
            };
            
            manager.track_index_update(update1.clone()).unwrap();
            manager.commit_index_transaction(1).unwrap();
            
            manager.track_index_update(update2).unwrap();
            // Don't commit transaction 2 - simulate crash
        }

        // Create new manager instance to test recovery
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            manager.recover_from_wal().unwrap();
            
            // Verify the committed transaction persisted and uncommitted was rolled back
            let index = manager.get_index("test_table", "test_column").unwrap();
            match index {
                IndexType::BTree(btree) => {
                    assert!(btree.search(vec![1, 2, 3]).is_some()); // Initial entry exists
                    assert!(btree.search(vec![4, 5, 6]).is_some()); // Committed entry exists
                    assert!(btree.search(vec![7, 8, 9]).is_none()); // Uncommitted entry doesn't exist
                },
                _ => panic!("Expected BTree index"),
            }
        }
    }

    #[test]
    fn test_index_recovery_with_updates() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test_index").to_str().unwrap().to_string();
        
        // Create initial manager and add some data
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            let mut btree = BTreeIndex::new();
            btree.add_entry(vec![1, 2, 3], 1);
            manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
            
            // Transaction 1 - Update value
            let update = IndexUpdate {
                table_name: "test_table".to_string(),
                column_name: "test_column".to_string(),
                old_value: Some(vec![1, 2, 3]),
                new_value: Some(vec![4, 5, 6]),
                row_id: 1,
                transaction_id: 1,
                operation_type: IndexOperationType::Update,
            };
            
            manager.track_index_update(update).unwrap();
            // Don't commit - simulate crash
        }

        // Create new manager instance to test recovery
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            manager.recover_from_wal().unwrap();
            
            // Verify the update was rolled back
            let index = manager.get_index("test_table", "test_column").unwrap();
            match index {
                IndexType::BTree(btree) => {
                    assert!(btree.search(vec![1, 2, 3]).is_some()); // Original value restored
                    assert!(btree.search(vec![4, 5, 6]).is_none()); // Updated value rolled back
                },
                _ => panic!("Expected BTree index"),
            }
        }
    }

    #[test]
    fn test_index_recovery_with_deletes() {
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir.path().join("test_index").to_str().unwrap().to_string();
        
        // Create initial manager and add some data
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            let mut btree = BTreeIndex::new();
            btree.add_entry(vec![1, 2, 3], 1);
            manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
            
            // Transaction 1 - Delete value
            let update = IndexUpdate {
                table_name: "test_table".to_string(),
                column_name: "test_column".to_string(),
                old_value: Some(vec![1, 2, 3]),
                new_value: None,
                row_id: 1,
                transaction_id: 1,
                operation_type: IndexOperationType::Delete,
            };
            
            manager.track_index_update(update).unwrap();
            // Don't commit - simulate crash
        }

        // Create new manager instance to test recovery
        {
            let mut manager = OnDiskIndexManager::new(index_path.clone());
            manager.recover_from_wal().unwrap();
            
            // Verify the delete was rolled back
            let index = manager.get_index("test_table", "test_column").unwrap();
            match index {
                IndexType::BTree(btree) => {
                    assert!(btree.search(vec![1, 2, 3]).is_some()); // Original value still exists
                },
                _ => panic!("Expected BTree index"),
            }
        }
    }
} 