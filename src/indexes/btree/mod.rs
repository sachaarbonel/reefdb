use std::collections::BTreeMap;
use std::collections::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BTreeIndex {
    // Map of column value to set of row IDs
    index: BTreeMap<Vec<u8>, HashSet<usize>>,
}

impl BTreeIndex {
    pub fn new() -> Self {
        BTreeIndex {
            index: BTreeMap::new(),
        }
    }

    pub fn add_entry(&mut self, value: Vec<u8>, row_id: usize) {
        self.index
            .entry(value)
            .or_insert_with(HashSet::new)
            .insert(row_id);
    }

    pub fn remove_entry(&mut self, value: Vec<u8>, row_id: usize) {
        if let Some(rows) = self.index.get_mut(&value) {
            rows.remove(&row_id);
            if rows.is_empty() {
                self.index.remove(&value);
            }
        }
    }

    pub fn search(&self, value: Vec<u8>) -> Option<&HashSet<usize>> {
        println!("BTreeIndex::search - Searching for value: {:?}", value);
        println!("BTreeIndex::search - Current index contents: {:?}", self.index);
        let result = self.index.get(&value);
        println!("BTreeIndex::search - Found result: {:?}", result);
        result
    }

    pub fn range_search(&self, start: Vec<u8>, end: Vec<u8>) -> HashSet<usize> {
        let mut results = HashSet::new();
        for rows in self.index.range(start..=end) {
            results.extend(rows.1.iter());
        }
        results
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<Vec<u8>, HashSet<usize>> {
        self.index.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let index = BTreeIndex::new();
        assert!(index.index.is_empty());
    }

    #[test]
    fn test_add_entry() {
        let mut index = BTreeIndex::new();
        index.add_entry(vec![1, 2, 3], 1);
        
        assert_eq!(index.index.len(), 1);
        assert!(index.index.contains_key(&vec![1, 2, 3]));
        assert_eq!(index.index.get(&vec![1, 2, 3]).unwrap().len(), 1);
        assert!(index.index.get(&vec![1, 2, 3]).unwrap().contains(&1));
    }

    #[test]
    fn test_remove_entry() {
        let mut index = BTreeIndex::new();
        index.add_entry(vec![1, 2, 3], 1);
        index.add_entry(vec![1, 2, 3], 2);
        
        index.remove_entry(vec![1, 2, 3], 1);
        assert_eq!(index.index.get(&vec![1, 2, 3]).unwrap().len(), 1);
        assert!(index.index.get(&vec![1, 2, 3]).unwrap().contains(&2));
        
        index.remove_entry(vec![1, 2, 3], 2);
        assert!(!index.index.contains_key(&vec![1, 2, 3]));
    }

    #[test]
    fn test_search() {
        let mut index = BTreeIndex::new();
        index.add_entry(vec![1, 2, 3], 1);
        index.add_entry(vec![1, 2, 3], 2);
        
        let result = index.search(vec![1, 2, 3]);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);
        assert!(result.unwrap().contains(&1));
        assert!(result.unwrap().contains(&2));
        
        assert!(index.search(vec![4, 5, 6]).is_none());
    }

    #[test]
    fn test_range_search() {
        let mut index = BTreeIndex::new();
        index.add_entry(vec![1], 1);
        index.add_entry(vec![2], 2);
        index.add_entry(vec![3], 3);
        
        let result = index.range_search(vec![1], vec![2]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }
}