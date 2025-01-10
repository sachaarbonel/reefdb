use super::btree::BTreeIndex;
use crate::fts::search::Search;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::fmt::Debug;
use std::collections::HashSet;
use serde::{Serialize, Deserialize};
use crate::fts::default::DefaultSearchIdx;

pub trait SearchIndex: Debug {
    type NewArgs: Clone;
    fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize>;
    fn add_column(&mut self, table: &str, column: &str);
    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str);
    fn remove_document(&mut self, table: &str, column: &str, row_id: usize);
    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str);
}

impl<T: Search + Debug> SearchIndex for T 
where
    T::NewArgs: Clone
{
    type NewArgs = T::NewArgs;
    fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize> {
        T::search(self, table, column, query)
    }
    fn add_column(&mut self, table: &str, column: &str) {
        T::add_column(self, table, column)
    }
    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        T::add_document(self, table, column, row_id, text)
    }
    fn remove_document(&mut self, table: &str, column: &str, row_id: usize) {
        T::remove_document(self, table, column, row_id)
    }
    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        T::update_document(self, table, column, row_id, text)
    }
}

pub trait IndexManager<T> {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType<T>);
    fn drop_index(&mut self, table: &str, column: &str);
    fn get_index(&self, table: &str, column: &str) -> Option<&IndexType<T>>;
    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize);
}

#[derive(Debug)]
pub enum IndexType<T> {
    BTree(BTreeIndex),
    GIN(Box<dyn SearchIndex<NewArgs = T>>),
}

impl<T> Serialize for IndexType<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            IndexType::BTree(btree) => btree.serialize(serializer),
            IndexType::GIN(_) => unimplemented!("GIN index serialization not supported"),
        }
    }
}

impl<'de, T> Deserialize<'de> for IndexType<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let btree = BTreeIndex::deserialize(deserializer)?;
        Ok(IndexType::BTree(btree))
    }
}

impl<T> Clone for IndexType<T> {
    fn clone(&self) -> Self {
        match self {
            IndexType::BTree(btree) => IndexType::BTree(btree.clone()),
            IndexType::GIN(_) => unimplemented!("GIN index cloning not supported"),
        }
    }
}

#[derive(Debug)]
pub struct DefaultIndexManager<T> {
    indexes: HashMap<String, HashMap<String, IndexType<T>>>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for DefaultIndexManager<T> {
    fn clone(&self) -> Self {
        DefaultIndexManager {
            indexes: self.indexes.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> DefaultIndexManager<T> {
    pub fn new() -> DefaultIndexManager<T> {
        DefaultIndexManager {
            indexes: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

impl<T> IndexManager<T> for DefaultIndexManager<T> {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType<T>) {
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

    fn get_index(&self, table: &str, column: &str) -> Option<&IndexType<T>> {
        self.indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(column))
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            if let Some(IndexType::BTree(index)) = table_indexes.get_mut(column) {
                index.remove_entry(old_value, row_id);
                index.add_entry(new_value, row_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::default::DefaultSearchIdx;

    #[test]
    fn test_btree_index() {
        let mut manager: DefaultIndexManager<()> = DefaultIndexManager::new();
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
        let mut manager: DefaultIndexManager<()> = DefaultIndexManager::new();
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
} 