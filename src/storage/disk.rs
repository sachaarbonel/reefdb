use crate::sql::column_def::ColumnDef;
use crate::sql::data_value::DataValue;
use crate::sql::data_type::DataType;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use super::Storage;
use crate::error::ReefDBError;
use crate::sql::constraints::constraint::Constraint;
use crate::indexes::{IndexManager, IndexType, disk::OnDiskIndexManager};
use crate::indexes::index_manager::IndexUpdate;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OnDiskStorage {
    file_path: String,
    tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
    index_manager: OnDiskIndexManager,
}

impl OnDiskStorage {
    pub fn new(file_path: String) -> Self {
        let path = Path::new(&file_path);
        let index_path = format!("{}.idx", file_path);

        let mut tables = HashMap::new();
        if path.exists() {
            let mut file = File::open(path).unwrap();
            let mut buffer = Vec::new();
            if file.read_to_end(&mut buffer).unwrap() > 0 {
                tables = deserialize(&buffer).unwrap();
            }
        }

        OnDiskStorage {
            file_path,
            tables,
            index_manager: OnDiskIndexManager::new(index_path),
        }
    }

    pub fn save(&self) {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_path)
            .unwrap();
        let mut writer = BufWriter::new(file);
        let buffer = serialize(&self.tables).unwrap();
        writer.write_all(&buffer).unwrap();
    }

    pub fn sync(&self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_path)?;
        let mut writer = BufWriter::new(file);
        let buffer = serialize(&self.tables).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writer.write_all(&buffer)?;
        writer.flush()?;
        writer.get_ref().sync_all()
    }
}

impl Storage for OnDiskStorage {
    type NewArgs = String;

    fn new(args: Self::NewArgs) -> Self {
        OnDiskStorage::new(args)
    }

    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        row: Vec<Vec<DataValue>>,
    ) {
        self.tables.insert(table_name, (columns, row));
        // Ensure changes are persisted to disk
        self.sync().unwrap_or_else(|e| {
            eprintln!("Failed to sync changes to disk: {}", e);
        });
    }

    fn get_table(
        &mut self,
        table_name: &str,
    ) -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get_mut(table_name)
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) -> Result<usize, ReefDBError> {
        if let Some((columns, rows)) = self.get_table(table_name) {
            // Validate constraints
            for (i, (column, value)) in columns.iter().zip(row.iter()).enumerate() {
                // Check UNIQUE constraint
                if column.constraints.contains(&Constraint::Unique) {
                    for existing_row in rows.iter() {
                        if existing_row[i] == *value {
                            return Err(ReefDBError::Other(format!(
                                "Unique constraint violation for column {} with value {:?}",
                                column.name, value
                            )));
                        }
                    }
                }
                
                // Check NOT NULL constraint
                if column.constraints.contains(&Constraint::NotNull) {
                    if let DataValue::Text(text) = value {
                        if text.is_empty() {
                            return Err(ReefDBError::Other(format!(
                                "NOT NULL constraint violation for column {}",
                                column.name
                            )));
                        }
                    }
                }
                
                // Check PRIMARY KEY constraint
                if column.constraints.contains(&Constraint::PrimaryKey) {
                    for existing_row in rows.iter() {
                        if existing_row[i] == *value {
                            return Err(ReefDBError::Other(format!(
                                "Primary key violation for column {} with value {:?}",
                                column.name, value
                            )));
                        }
                    }
                }
            }

            // Get the rowid before modifying the table
            let rowid = rows.len() + 1;

            // Add the new row to the table
            rows.push(row);

            // Save after all modifications are done
            let _ = self.sync();

            // Return the rowid (1-based index)
            Ok(rowid)
        } else {
            // Return an error if the table doesn't exist
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn update_table(
        &mut self,
        table_name: &str,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        let (schema, rows) = self.get_table(table_name).unwrap();
        let mut last_updated_row_id = 0;

        for (idx, row) in rows.iter_mut().enumerate() {
            let matches_where = if let Some((column, value)) = &where_clause {
                let column_idx = schema.iter().position(|c| c.name == *column).unwrap();
                row[column_idx] == *value
            } else {
                true
            };

            if matches_where {
                for (column, value) in &updates {
                    let column_idx = schema.iter().position(|c| c.name == *column).unwrap();
                    row[column_idx] = value.clone();
                }
                last_updated_row_id = idx + 1; // Convert to 1-based index
            }
        }
        self.save();
        last_updated_row_id
    }

    fn delete_table(
        &mut self,
        table_name: &str,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        //TODO: Option<usize>
        // None if table doesn't exist or column not found
        let (schema, rows) = self.get_table(table_name).unwrap();
        let mut idx = 0;

        for row in rows.iter_mut() {
            if let Some((column, value)) = &where_clause {
                let column_idx = schema.iter().position(|c| c.name == *column).unwrap();
                if row[column_idx] != *value {
                    idx += 1;
                    continue;
                }
            }
            idx += 1;
        }
        self.save();
        idx
    }

    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
    }

    fn remove_table(&mut self, table_name: &str) -> bool {
        let exists = self.tables.remove(table_name).is_some();
        if exists {
            self.save();
        }
        exists
    }

    fn add_column(&mut self, table_name: &str, column_def: ColumnDef) -> Result<(), ReefDBError> {
        if let Some((schema, data)) = self.tables.get_mut(table_name) {
            schema.push(column_def.clone());
            // Add default value for the new column in all existing rows
            let default_value = match column_def.data_type {
                DataType::Text | DataType::TSVector => DataValue::Text(String::new()),
                DataType::Integer => DataValue::Integer(0),
            };
            for row in data.iter_mut() {
                row.push(default_value.clone());
            }
            self.save();
            Ok(())
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn drop_column(&mut self, table_name: &str, column_name: &str) -> Result<(), ReefDBError> {
        if let Some((schema, data)) = self.tables.get_mut(table_name) {
            if let Some(idx) = schema.iter().position(|col| col.name == column_name) {
                schema.remove(idx);
                // Remove the column data from all rows
                for row in data.iter_mut() {
                    row.remove(idx);
                }
                self.save();
                Ok(())
            } else {
                Err(ReefDBError::ColumnNotFound(column_name.to_string()))
            }
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn rename_column(&mut self, table_name: &str, old_name: &str, new_name: &str) -> Result<(), ReefDBError> {
        if let Some((schema, _)) = self.tables.get_mut(table_name) {
            if let Some(col) = schema.iter_mut().find(|col| col.name == old_name) {
                col.name = new_name.to_string();
                self.save();
                Ok(())
            } else {
                Err(ReefDBError::ColumnNotFound(old_name.to_string()))
            }
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn drop_table(&mut self, table_name: &str) {
        if self.tables.remove(table_name).is_some() {
            self.save();
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clear(&mut self) {
        self.tables.clear();
        // Also clear the on-disk storage
        let file_path = &self.file_path;
        if let Some(parent) = std::path::Path::new(file_path).parent() {
            if let Ok(entries) = std::fs::read_dir(parent) {
                for entry in entries.flatten() {
                    if let Ok(file_type) = entry.file_type() {
                        if file_type.is_file() {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }
    }

    fn get_all_tables(&self) -> &HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        &self.tables
    }
}

impl IndexManager for OnDiskStorage {
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
    use crate::sql::{constraints::constraint::Constraint, data_type::DataType};
    use tempfile::NamedTempFile;

    #[test]
    fn test_disk_storage() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        
        // Create and populate storage
        {
            let mut storage = OnDiskStorage::new(file_path.clone());
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
            let mut storage = OnDiskStorage::new(file_path);
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
