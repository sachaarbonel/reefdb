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
use crate::indexes::{IndexManager, IndexType};
use crate::indexes::index_manager::IndexUpdate;
use crate::fts::search::Search;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskIndexManager {
    file_path: String,
    indexes: HashMap<String, HashMap<String, IndexType>>,
    #[serde(skip)]
    pending_updates: HashMap<u64, Vec<IndexUpdate>>,
    #[serde(skip)]
    active_transactions: std::collections::HashSet<u64>,
}

impl OnDiskIndexManager {
    pub fn new(file_path: String) -> Self {
        let index_file_path = format!("{}.index", file_path);
        let indexes = if Path::new(&index_file_path).exists() {
            match File::open(&index_file_path) {
                Ok(mut file) => {
                    let mut contents = Vec::new();
                    if file.read_to_end(&mut contents).is_ok() {
                        match deserialize(&contents) {
                            Ok(loaded_manager) => {
                                let OnDiskIndexManager { indexes, .. } = loaded_manager;
                                indexes
                            }
                            Err(_) => HashMap::new(),
                        }
                    } else {
                        HashMap::new()
                    }
                }
                Err(_) => HashMap::new(),
            }
        } else {
            HashMap::new()
        };

        OnDiskIndexManager {
            file_path: index_file_path,
            indexes,
            pending_updates: HashMap::new(),
            active_transactions: std::collections::HashSet::new(),
        }
    }

    fn save(&self) -> Result<(), ReefDBError> {
        let encoded_data = serialize(self)
            .map_err(|e| ReefDBError::Other(format!("Serialization error: {}", e)))?;
        let mut file = File::create(&self.file_path)
            .map_err(|e| ReefDBError::IoError(e.to_string()))?;
        file.write_all(&encoded_data)
            .map_err(|e| ReefDBError::IoError(e.to_string()))?;
        Ok(())
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
        self.indexes
            .get(table)
            .ok_or_else(|| ReefDBError::TableNotFound(table.to_string()))?
            .get(column)
            .ok_or_else(|| ReefDBError::ColumnNotFound(column.to_string()))
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) -> Result<(), ReefDBError> {
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
                if !new_value.is_empty() {
                    gin.add_document(table, column, row_id, std::str::from_utf8(&new_value).unwrap_or_default());
                }
            }
        }
        self.save()?;
        Ok(())
    }

    fn track_index_update(&mut self, update: IndexUpdate) -> Result<(), ReefDBError> {
        let transaction_updates = self.pending_updates.entry(update.transaction_id).or_insert_with(Vec::new);
        transaction_updates.push(update.clone());
        self.active_transactions.insert(update.transaction_id);
        
        match (update.old_value, update.new_value) {
            (Some(old_value), Some(new_value)) => {
                self.update_index(&update.table_name, &update.column_name, old_value, new_value, update.row_id)?;
            }
            (None, Some(new_value)) => {
                self.update_index(&update.table_name, &update.column_name, vec![], new_value, update.row_id)?;
            }
            (Some(old_value), None) => {
                self.update_index(&update.table_name, &update.column_name, old_value, vec![], update.row_id)?;
            }
            (None, None) => {}
        }
        self.save()?;
        Ok(())
    }

    fn commit_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        self.pending_updates.remove(&transaction_id);
        self.active_transactions.remove(&transaction_id);
        self.save()?;
        Ok(())
    }

    fn rollback_index_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if let Some(updates) = self.pending_updates.remove(&transaction_id) {
            for update in updates.iter().rev() {
                match (&update.old_value, &update.new_value) {
                    (Some(old_value), Some(_)) => {
                        self.update_index(&update.table_name, &update.column_name, vec![], old_value.clone(), update.row_id)?;
                    }
                    (None, Some(_)) => {
                        self.update_index(&update.table_name, &update.column_name, vec![], vec![], update.row_id)?;
                    }
                    (Some(old_value), None) => {
                        self.update_index(&update.table_name, &update.column_name, vec![], old_value.clone(), update.row_id)?;
                    }
                    (None, None) => {}
                }
            }
        }
        self.active_transactions.remove(&transaction_id);
        self.save()?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OnDiskStorage {
    file_path: String,
    tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
    index_manager: OnDiskIndexManager,
}

impl OnDiskStorage {
    pub fn new(file_path: String) -> Self {
        let tables = if Path::new(&file_path).exists() {
            println!("Loading existing file: {}", file_path);
            let mut file = File::open(&file_path).unwrap();
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).unwrap();
            println!("Read {} bytes", contents.len());
            let tables = deserialize(&contents).unwrap_or_default();
            println!("Loaded tables: {:?}", tables);
            tables
        } else {
            println!("File does not exist: {}", file_path);
            HashMap::new()
        };

        OnDiskStorage {
            file_path: file_path.clone(),
            tables,
            index_manager: OnDiskIndexManager::new(file_path),
        }
    }

    pub fn save(&self) {
        println!("Saving tables: {:?}", self.tables);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_path)
            .unwrap();
        let mut writer = BufWriter::new(file);
        let serialized = serialize(&self.tables).unwrap();
        println!("Writing {} bytes", serialized.len());
        writer.write_all(&serialized).unwrap();
        writer.flush().unwrap();
    }

    pub fn sync(&self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_path)?;
        let mut writer = BufWriter::new(file);
        let serialized = serialize(&self.tables).unwrap();
        writer.write_all(&serialized)?;
        writer.flush()?;
        Ok(())
    }
}

unsafe impl Send for OnDiskStorage {}
unsafe impl Sync for OnDiskStorage {}

impl Storage for OnDiskStorage {
    type NewArgs = String;

    fn new(args: Self::NewArgs) -> Self {
        Self::new(args)
    }

    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        row: Vec<Vec<DataValue>>,
    ) {
        self.tables.insert(table_name, (columns, row));
        // Ensure changes are persisted to disk
        self.save();
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
                DataType::Text => DataValue::Text(String::new()),
                DataType::Integer => DataValue::Integer(0),
                DataType::TSVector => DataValue::Text(String::new()),
                DataType::Boolean => DataValue::Boolean(false),
                DataType::Float => DataValue::Float(0.0),
                DataType::Date => DataValue::Date(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
                DataType::Timestamp => DataValue::Timestamp(chrono::NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                DataType::Null => DataValue::Null,
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
