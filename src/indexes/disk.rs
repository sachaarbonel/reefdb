use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use super::index_manager::{IndexType, IndexManager};

#[derive(Debug, Serialize, Deserialize)]
pub struct OnDiskIndexManager {
    file_path: String,
    indexes: HashMap<String, HashMap<String, IndexType>>,
}

impl Clone for OnDiskIndexManager {
    fn clone(&self) -> Self {
        OnDiskIndexManager {
            file_path: self.file_path.clone(),
            indexes: self.indexes.clone(),
        }
    }
}

impl OnDiskIndexManager {
    pub fn new(file_path: String) -> Self {
        let path = Path::new(&file_path);
        let mut indexes = HashMap::new();

        if path.exists() {
            if let Ok(mut file) = File::open(path) {
                let mut buffer = Vec::new();
                if file.read_to_end(&mut buffer).is_ok() {
                    if let Ok(loaded_indexes) = bincode::deserialize(&buffer) {
                        indexes = loaded_indexes;
                    }
                }
            }
        }

        OnDiskIndexManager {
            file_path,
            indexes,
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)?;
            
        let mut writer = BufWriter::new(file);
        let buffer = bincode::serialize(&self.indexes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writer.write_all(&buffer)?;
        writer.flush()?;
        Ok(())
    }
}

impl IndexManager for OnDiskIndexManager {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) {
        self.indexes
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .insert(column.to_string(), index_type);
        self.save().unwrap();
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        if let Some(table_indexes) = self.indexes.get_mut(table) {
            table_indexes.remove(column);
            self.save().unwrap();
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
                        self.save().unwrap();
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
    use crate::indexes::btree::BTreeIndex;
    use tempfile::NamedTempFile;

    #[test]
    fn test_on_disk_index_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        
        // Create and populate index manager
        {
            let mut manager = OnDiskIndexManager::new(file_path.clone());
            let mut btree = BTreeIndex::new();
            btree.add_entry(vec![1, 2, 3], 1);
            btree.add_entry(vec![4, 5, 6], 2);
            
            manager.create_index("users", "age", IndexType::BTree(btree));
        }

        // Create new manager instance and verify persistence
        {
            let manager = OnDiskIndexManager::new(file_path);
            if let Some(IndexType::BTree(index)) = manager.get_index("users", "age") {
                assert!(index.search(vec![1, 2, 3]).unwrap().contains(&1));
                assert!(index.search(vec![4, 5, 6]).unwrap().contains(&2));
            } else {
                panic!("Index not found or wrong type");
            }
        }
    }
} 