use crate::sql::column_def::ColumnDef;
use crate::sql::data_value::DataValue;
use crate::error::ReefDBError;
use crate::indexes::{IndexManager, IndexType};
use crate::indexes::index_manager::{IndexUpdate, DefaultIndexManager};
use memmap2::{MmapMut, MmapOptions};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use bincode::{serialize, deserialize};
use std::any::Any;
use super::Storage;
use crate::sql::data_type::DataType;

#[derive(Debug)]
pub struct MmapStorage {
    file_path: String,
    tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
    index_manager: DefaultIndexManager,
    #[allow(dead_code)]
    mmap: Option<MmapMut>,
}

impl Clone for MmapStorage {
    fn clone(&self) -> Self {
        MmapStorage {
            file_path: self.file_path.clone(),
            tables: self.tables.clone(),
            index_manager: self.index_manager.clone(),
            mmap: None,
        }
    }
}

impl MmapStorage {
    pub fn new(file_path: String) -> Self {
        let tables = if Path::new(&file_path).exists() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)
                .unwrap();
            
            // Ensure file has some content
            file.set_len(1024 * 1024).unwrap(); // 1MB initial size
            
            let mmap = unsafe { MmapOptions::new().map_mut(&file).unwrap() };
            
            if mmap.len() > 0 {
                match deserialize(&mmap[..]) {
                    Ok(tables) => tables,
                    Err(_) => HashMap::new(),
                }
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        MmapStorage {
            file_path,
            tables,
            index_manager: DefaultIndexManager::new(),
            mmap: None,
        }
    }

    fn save(&mut self) -> Result<(), ReefDBError> {
        let serialized = serialize(&self.tables)
            .map_err(|e| ReefDBError::Other(format!("Serialization error: {}", e)))?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.file_path)
            .map_err(|e| ReefDBError::IoError(e.to_string()))?;

        // Ensure file is large enough
        let required_size = serialized.len() as u64;
        file.set_len(required_size)
            .map_err(|e| ReefDBError::IoError(e.to_string()))?;

        // Create new memory mapping
        let mut mmap = unsafe { 
            MmapOptions::new()
                .len(serialized.len())
                .map_mut(&file)
                .map_err(|e| ReefDBError::IoError(e.to_string()))?
        };

        // Write data to memory map
        mmap.copy_from_slice(&serialized);
        
        // Sync changes to disk
        mmap.flush()
            .map_err(|e| ReefDBError::IoError(e.to_string()))?;

        self.mmap = Some(mmap);
        Ok(())
    }

    fn get_default_value(data_type: &DataType) -> DataValue {
        match data_type {
            DataType::Integer => DataValue::Integer(0),
            DataType::Text => DataValue::Text(String::new()),
            DataType::TSVector => DataValue::Text(String::new()),
        }
    }
}

impl Storage for MmapStorage {
    type NewArgs = String;

    fn new(args: Self::NewArgs) -> Self {
        Self::new(args)
    }

    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        rows: Vec<Vec<DataValue>>,
    ) {
        self.tables.insert(table_name, (columns, rows));
        let _ = self.save();
    }

    fn get_table(
        &mut self,
        table_name: &str,
    ) -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get_mut(table_name)
    }

    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) -> Result<usize, ReefDBError> {
        let len = if let Some((_, rows)) = self.tables.get_mut(table_name) {
            rows.push(row);
            rows.len()
        } else {
            return Err(ReefDBError::TableNotFound(table_name.to_string()));
        };
        let _ = self.save();
        Ok(len)
    }

    fn update_table(
        &mut self,
        table_name: &str,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        let mut updated_count = 0;
        if let Some((columns, rows)) = self.tables.get_mut(table_name) {
            for row in rows.iter_mut() {
                let should_update = where_clause.as_ref().map_or(true, |(col, val)| {
                    if let Some(col_idx) = columns.iter().position(|c| c.name == *col) {
                        &row[col_idx] == val
                    } else {
                        false
                    }
                });

                if should_update {
                    for (col, val) in &updates {
                        if let Some(col_idx) = columns.iter().position(|c| c.name == *col) {
                            row[col_idx] = val.clone();
                        }
                    }
                    updated_count += 1;
                }
            }
            let _ = self.save();
        }
        updated_count
    }

    fn delete_table(
        &mut self,
        table_name: &str,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        let mut deleted_count = 0;
        if let Some((columns, rows)) = self.tables.get_mut(table_name) {
            let initial_len = rows.len();
            rows.retain(|row| {
                let should_keep = where_clause.as_ref().map_or(true, |(col, val)| {
                    if let Some(col_idx) = columns.iter().position(|c| c.name == *col) {
                        &row[col_idx] != val
                    } else {
                        true
                    }
                });
                should_keep
            });
            deleted_count = initial_len - rows.len();
            let _ = self.save();
        }
        deleted_count
    }

    fn remove_table(&mut self, table_name: &str) -> bool {
        let exists = self.tables.remove(table_name).is_some();
        if exists {
            let _ = self.save();
        }
        exists
    }

    fn add_column(&mut self, table_name: &str, column_def: ColumnDef) -> Result<(), ReefDBError> {
        if let Some((columns, rows)) = self.tables.get_mut(table_name) {
            let default_value = Self::get_default_value(&column_def.data_type);
            columns.push(column_def.clone());
            for row in rows.iter_mut() {
                row.push(default_value.clone());
            }
            let _ = self.save();
            Ok(())
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn drop_column(&mut self, table_name: &str, column_name: &str) -> Result<(), ReefDBError> {
        if let Some((columns, rows)) = self.tables.get_mut(table_name) {
            if let Some(index) = columns.iter().position(|c| c.name == column_name) {
                columns.remove(index);
                for row in rows.iter_mut() {
                    row.remove(index);
                }
                let _ = self.save();
                Ok(())
            } else {
                Err(ReefDBError::ColumnNotFound(column_name.to_string()))
            }
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn rename_column(&mut self, table_name: &str, old_name: &str, new_name: &str) -> Result<(), ReefDBError> {
        if let Some((columns, _)) = self.tables.get_mut(table_name) {
            if let Some(col) = columns.iter_mut().find(|c| c.name == old_name) {
                col.name = new_name.to_string();
                let _ = self.save();
                Ok(())
            } else {
                Err(ReefDBError::ColumnNotFound(old_name.to_string()))
            }
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn drop_table(&mut self, table_name: &str) {
        self.tables.remove(table_name);
        let _ = self.save();
    }

    fn clear(&mut self) {
        self.tables.clear();
        let _ = self.save();
    }

    fn get_all_tables(&self) -> &HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        &self.tables
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl IndexManager for MmapStorage {
    fn create_index(&mut self, table: &str, column: &str, index_type: IndexType) -> Result<(), ReefDBError> {
        self.index_manager.create_index(table, column, index_type)
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        self.index_manager.drop_index(table, column)
    }

    fn get_index(&self, table: &str, column: &str) -> Result<&IndexType, ReefDBError> {
        self.index_manager.get_index(table, column)
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) -> Result<(), ReefDBError> {
        self.index_manager.update_index(table, column, old_value, new_value, row_id)
    }

    fn track_index_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError> {
        self.index_manager.track_index_update(update)
    }

    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        self.index_manager.commit_index_transaction(transaction_id)
    }

    fn rollback_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        self.index_manager.rollback_index_transaction(transaction_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::constraints::constraint::Constraint;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmap_storage() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        
        // Create and populate storage
        {
            let mut storage = MmapStorage::new(file_path.clone());
            let columns = vec![
                ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
                ColumnDef::new("name", DataType::Text, vec![]),
                ColumnDef::new("age", DataType::Integer, vec![]),
            ];
            let rows = vec![
                vec![
                    DataValue::Integer(1),
                    DataValue::Text("John".to_string()),
                    DataValue::Integer(20),
                ],
                vec![
                    DataValue::Integer(2),
                    DataValue::Text("Jane".to_string()),
                    DataValue::Integer(25),
                ],
            ];
            storage.insert_table("users".to_string(), columns, rows);
        }

        // Create new storage instance and verify persistence
        {
            let mut storage = MmapStorage::new(file_path);
            let (schema, rows) = storage.get_table("users").unwrap();
            assert_eq!(schema.len(), 3);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].len(), 3);
            assert_eq!(rows[1].len(), 3);
            assert_eq!(rows[0][0], DataValue::Integer(1));
            assert_eq!(rows[0][1], DataValue::Text("John".to_string()));
            assert_eq!(rows[0][2], DataValue::Integer(20));
            assert_eq!(rows[1][0], DataValue::Integer(2));
            assert_eq!(rows[1][1], DataValue::Text("Jane".to_string()));
            assert_eq!(rows[1][2], DataValue::Integer(25));
        }
    }
} 