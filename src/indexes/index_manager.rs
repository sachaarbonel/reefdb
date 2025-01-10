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
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType);
    fn drop_index(&mut self, table: &str, column: &str);
    fn get_index(&self, table: &str, column: &str) -> Option<&IndexType>;
    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize);
}

#[derive(Debug)]
pub struct DefaultIndexManager {
    indexes: HashMap<String, HashMap<String, IndexType>>,
}

impl Clone for DefaultIndexManager {
    fn clone(&self) -> Self {
        DefaultIndexManager {
            indexes: self.indexes.clone(),
        }
    }
}

impl DefaultIndexManager {
    pub fn new() -> DefaultIndexManager {
        DefaultIndexManager {
            indexes: HashMap::new(),
        }
    }
}

impl IndexManager for DefaultIndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) {
        self.indexes
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), index_type);
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            table_indexes.remove(column);
        }
    }

    fn get_index(&self, table: &str, column: &str) -> Option<&IndexType> {
        self.indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(column))
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            if let Some(index) = table_indexes.get_mut(column) {
                match index {
                    IndexType::BTree(btree) => {
                        btree.remove_entry(old_value, row_id);
                        btree.add_entry(new_value, row_id);
                    }
                    IndexType::GIN(gin) => {
                        // GIN indexes don't support direct value updates
                        // They are updated through add_document/remove_document
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_index() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add some test data
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 2);
        
        manager.create_index("users", "age", IndexType::BTree(btree));
        
        // Test searching
        if let Some(IndexType::BTree(index)) = manager.get_index("users", "age") {
            assert!(index.search(vec![1, 2, 3]).unwrap().contains(&1));
            assert!(index.search(vec![4, 5, 6]).unwrap().contains(&2));
        }
    }

    #[test]
    fn test_index_crud() {
        let mut manager = DefaultIndexManager::new();
        let btree = BTreeIndex::new();
        
        // Create
        manager.create_index("users", "age", IndexType::BTree(btree));
        assert!(manager.get_index("users", "age").is_some());
        
        // Update
        manager.update_index("users", "age", vec![1], vec![2], 1);
        
        // Drop
        manager.drop_index("users", "age");
        assert!(manager.get_index("users", "age").is_none());
    }

    #[test]
    fn test_gin_index_serialization() {
        let mut gin = GinIndex::<DefaultTokenizer>::new();
        gin.add_column("users", "bio");
        gin.add_document("users", "bio", 1, "Hello world");
        gin.add_document("users", "bio", 2, "Goodbye world");

        let index_type = IndexType::GIN(gin);
        
        // Serialize
        let serialized = bincode::serialize(&index_type).unwrap();
        
        // Deserialize
        let deserialized: IndexType = bincode::deserialize(&serialized).unwrap();
        
        // Verify
        if let IndexType::GIN(gin) = deserialized {
            let results = gin.search("users", "bio", "world");
            assert_eq!(results.len(), 2);
            assert!(results.contains(&1));
            assert!(results.contains(&2));
        } else {
            panic!("Expected GIN index");
        }
    }
} 