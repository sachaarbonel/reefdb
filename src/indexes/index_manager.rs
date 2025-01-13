use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::fmt::Debug;
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

#[derive(Debug, Clone)]
pub struct IndexUpdate {
    pub table_name: String,
    pub column_name: String,
    pub old_value: Option<Vec<u8>>,
    pub new_value: Option<Vec<u8>>,
    pub row_id: usize,
    pub transaction_id: u64,
}

#[derive(Debug, Clone)]
pub struct DefaultIndexManager {
    indexes: HashMap<String, HashMap<String, IndexType>>,
    pending_updates: HashMap<u64, Vec<IndexUpdate>>,
    active_transactions: HashSet<u64>,
}

impl DefaultIndexManager {
    pub fn new() -> DefaultIndexManager {
        DefaultIndexManager {
            indexes: HashMap::new(),
            pending_updates: HashMap::new(),
            active_transactions: HashSet::new(),
        }
    }

    fn get_index_internal(&self, table: &str, column: &str) -> Option<&IndexType> {
        println!("Getting index for table: {}, column: {}", table, column);
        println!("Available indexes: {:?}", self.indexes.keys().collect::<Vec<_>>());
        println!("Table indexes: {:?}", self.indexes.get(table).map(|t| t.keys().collect::<Vec<_>>()));
        self.indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(column))
    }

    pub fn track_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError> {
        // Track the transaction
        self.active_transactions.insert(update.transaction_id);
        
        // Store the update for potential rollback
        self.pending_updates
            .entry(update.transaction_id)
            .or_insert_with(Vec::new)
            .push(update.clone());

        // Apply the update immediately
        if let (Some(old_value), Some(new_value)) = (update.old_value, update.new_value) {
            self.update_index(&update.table_name, &update.column_name, old_value, new_value, update.row_id)?;
        }
        
        Ok(())
    }

    pub fn commit_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        // Just remove the transaction tracking data since changes are already applied
        self.pending_updates.remove(&transaction_id);
        self.active_transactions.remove(&transaction_id);
        Ok(())
    }

    pub fn rollback_transaction(&mut self, transaction_id: u64) {
        if let Some(updates) = self.pending_updates.remove(&transaction_id) {
            // Reverse the updates in LIFO order
            for update in updates.into_iter().rev() {
                if let (Some(old_value), Some(new_value)) = (update.old_value, update.new_value) {
                    // Swap old and new values to reverse the update
                    let _ = self.update_index(&update.table_name, &update.column_name, new_value, old_value, update.row_id);
                }
            }
        }
        self.active_transactions.remove(&transaction_id);
    }
}

impl IndexManager for DefaultIndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) -> Result<(), ReefDBError> {
        println!("Creating index for table: {}, column: {}", table, column);
        let table_indexes = self.indexes.entry(table.to_string()).or_insert_with(HashMap::new);
        table_indexes.insert(column.to_string(), index_type);
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
        Ok(())
    }

    fn track_index_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError> {
        println!("Tracking index update: {:?}", update);
        let transaction_updates = self.pending_updates.entry(update.transaction_id).or_insert_with(Vec::new);
        transaction_updates.push(update.clone());
        
        // Apply the update immediately
        match (update.old_value, update.new_value) {
            (Some(old_value), Some(new_value)) => {
                self.update_index(&update.table_name, &update.column_name, old_value, new_value, update.row_id)?;
            }
            (None, Some(new_value)) => {
                self.update_index(&update.table_name, &update.column_name, vec![], new_value, update.row_id)?;
            }
            (Some(old_value), None) => {
                // If new_value is None, we're deleting the entry
                self.update_index(&update.table_name, &update.column_name, old_value, vec![], update.row_id)?;
            }
            (None, None) => {
                // No-op if both values are None
            }
        }
        Ok(())
    }

    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        // Since we apply updates immediately in track_index_update, we just need to clean up
        self.pending_updates.remove(&transaction_id);
        Ok(())
    }

    fn rollback_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if let Some(updates) = self.pending_updates.remove(&transaction_id) {
            // Reverse the updates in reverse order
            for update in updates.into_iter().rev() {
                match (update.old_value, update.new_value) {
                    (Some(old_value), Some(new_value)) => {
                        // Swap old and new values to reverse the update
                        self.update_index(&update.table_name, &update.column_name, new_value, old_value, update.row_id)?;
                    }
                    (None, Some(new_value)) => {
                        // Delete the entry that was added
                        self.update_index(&update.table_name, &update.column_name, new_value, vec![], update.row_id)?;
                    }
                    (Some(old_value), None) => {
                        // Restore the deleted entry
                        self.update_index(&update.table_name, &update.column_name, vec![], old_value, update.row_id)?;
                    }
                    (None, None) => {
                        // No-op if both values are None
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexes::btree::BTreeIndex;

    #[test]
    fn test_btree_index() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add some test data
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 2);
        
        manager.create_index("users", "age", IndexType::BTree(btree)).unwrap();
        
        // Test searching
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            assert!(index.search(vec![1, 2, 3]).unwrap().contains(&1));
            assert!(index.search(vec![4, 5, 6]).unwrap().contains(&2));
        }
    }

    #[test]
    fn test_transaction_commit() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add initial data
        btree.add_entry(vec![1, 2, 3], 1);
        manager.create_index("users", "age", IndexType::BTree(btree)).unwrap();

        let transaction_id = 1;
        
        // Create update
        println!("Creating update...");
        let update = IndexUpdate {
            transaction_id,
            table_name: "users".to_string(),
            column_name: "age".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![7, 8, 9]),
            row_id: 1,
        };
        
        println!("Tracking update...");
        manager.track_index_update(update).unwrap();
        
        // Verify the update is immediately visible
        println!("Verifying pre-commit state...");
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9]");
            let old_result = index.search(vec![1, 2, 3]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3]");
        } else {
            panic!("Index not found after creation");
        }
        
        // Commit the transaction
        println!("Committing transaction...");
        manager.commit_index_transaction(transaction_id).unwrap();
        
        // Verify update remains visible after commit
        println!("Verifying post-commit state...");
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9] after commit");
            let old_result = index.search(vec![1, 2, 3]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3] after commit");
        } else {
            panic!("Index not found after commit");
        }
    }

    #[test]
    fn test_transaction_rollback() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add initial data
        btree.add_entry(vec![1, 2, 3], 1);
        manager.create_index("users", "age", IndexType::BTree(btree)).unwrap();

        // Create a transaction and track some updates
        let transaction_id = 1;
        
        // Track an update within the transaction
        let update = IndexUpdate {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![7, 8, 9]),
            row_id: 1,
            transaction_id,
        };
        
        manager.track_index_update(update).unwrap();
        
        // Verify the update is immediately visible
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9]");
            let old_result = index.search(vec![1, 2, 3]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3]");
        }
        
        // Rollback the transaction
        manager.rollback_index_transaction(transaction_id).unwrap();
        
        // Verify original value is restored after rollback
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![1, 2, 3]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [1,2,3] after rollback");
            let old_result = index.search(vec![7, 8, 9]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [7,8,9] after rollback");
        }
    }

    #[test]
    fn test_concurrent_transactions() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add initial data
        btree.add_entry(vec![1, 2, 3], 1);
        manager.create_index("users", "age", IndexType::BTree(btree)).unwrap();

        // Create two concurrent transactions
        let transaction_id1 = 1;
        let transaction_id2 = 2;
        
        // Track updates for both transactions
        let update1 = IndexUpdate {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
            old_value: Some(vec![1, 2, 3]),
            new_value: Some(vec![7, 8, 9]),
            row_id: 1,
            transaction_id: transaction_id1,
        };
        
        let update2 = IndexUpdate {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
            old_value: Some(vec![7, 8, 9]),  // Note: this should be the current value after update1
            new_value: Some(vec![4, 5, 6]),
            row_id: 1,
            transaction_id: transaction_id2,
        };
        
        // Apply first update
        manager.track_index_update(update1).unwrap();
        
        // Verify first update is visible
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9]");
            let old_result = index.search(vec![1, 2, 3]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3]");
        }
        
        // Apply second update
        manager.track_index_update(update2).unwrap();
        
        // Verify second update is visible
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![4, 5, 6]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [4,5,6]");
            let old_result = index.search(vec![7, 8, 9]);
            assert!(old_result.is_none() || !old_result.unwrap().contains(&1), "Expected not to find row_id 1 for value [7,8,9]");
        }
        
        // Commit first transaction, rollback second
        manager.commit_index_transaction(transaction_id1).unwrap();
        manager.rollback_index_transaction(transaction_id2).unwrap();
        
        // Verify final state - should be the value from transaction1 since transaction2 was rolled back
        if let Ok(IndexType::BTree(index)) = manager.get_index("users", "age") {
            let search_result = index.search(vec![7, 8, 9]);
            assert!(search_result.is_some() && search_result.unwrap().contains(&1), "Expected to find row_id 1 for value [7,8,9] after rollback");
            let old_result1 = index.search(vec![4, 5, 6]);
            assert!(old_result1.is_none() || !old_result1.unwrap().contains(&1), "Expected not to find row_id 1 for value [4,5,6] after rollback");
            let old_result2 = index.search(vec![1, 2, 3]);
            assert!(old_result2.is_none() || !old_result2.unwrap().contains(&1), "Expected not to find row_id 1 for value [1,2,3] after rollback");
        }
    }
} 