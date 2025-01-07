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
        self.index.get(&value)
    }

    pub fn range_search(&self, start: Vec<u8>, end: Vec<u8>) -> HashSet<usize> {
        let mut results = HashSet::new();
        for rows in self.index.range(start..=end) {
            results.extend(rows.1.iter());
        }
        results
    }
} 