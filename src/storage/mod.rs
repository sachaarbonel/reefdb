use std::collections::HashMap;
use crate::{sql::column_def::ColumnDef, sql::{data_value::DataValue, data_type::DataType}, error::ReefDBError};

pub mod disk;
pub mod memory;
pub mod mmap;

#[derive(Clone, Debug)]
pub struct TableStorage {
    pub tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
}

impl Default for TableStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl TableStorage {
    pub fn new() -> Self {
        TableStorage {
            tables: HashMap::new(),
        }
    }

    pub fn restore_from(&mut self, other: &TableStorage) {
        self.tables = other.tables.clone();
    }

    pub fn clone(&self) -> TableStorage {
        TableStorage {
            tables: self.tables.clone(),
        }
    }
}

pub trait Storage: std::any::Any {
    type NewArgs;
    fn new(args: Self::NewArgs) -> Self;
    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        row: Vec<Vec<DataValue>>,
    );

    fn get_table(&mut self, table_name: &str)
        -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)>;

    fn get_fts_columns(&self, table_name: &str) -> Vec<String> {
        if let Some((schema, _)) = self.get_table_ref(table_name) {
            schema
                .iter()
                .filter(|column_def| column_def.data_type == DataType::TSVector)
                .map(|column_def| column_def.name.clone())
                .collect()
        } else {
            vec![] // Return an empty Vec if the table doesn't exist
        }
    }
    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)>;
    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) -> Result<usize, ReefDBError>;

    fn update_table(
        &mut self,
        table_name: &str,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<(String, DataValue)>,
    ) -> usize;

    fn delete_table(
        &mut self,
        table_name: &str,
        where_clause: Option<(String, DataValue)>,
    ) -> usize;

    fn table_exists(&self, table_name: &str) -> bool;

    fn get_schema(&mut self, table_name: &str) -> Option<&mut Vec<ColumnDef>> {
        self.get_table(table_name).map(|(schema, _)| schema)
    }

    // non mutable
    fn get_schema_ref(&self, table_name: &str) -> Option<&Vec<ColumnDef>> {
        self.get_table_ref(table_name).map(|(schema, _)| schema)
    }

    fn remove_table(&mut self, table_name: &str) -> bool;

    fn add_column(&mut self, table_name: &str, column_def: ColumnDef) -> Result<(), ReefDBError>;
    fn drop_column(&mut self, table_name: &str, column_name: &str) -> Result<(), ReefDBError>;
    fn rename_column(&mut self, table_name: &str, old_name: &str, new_name: &str) -> Result<(), ReefDBError>;
    fn drop_table(&mut self, table_name: &str);

    fn as_any(&self) -> &dyn std::any::Any;

    // Clear all tables from storage
    fn clear(&mut self);

    // Get all tables and their data
    fn get_all_tables(&self) -> &HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>;

    // Restore storage state from a TableStorage
    fn restore_from(&mut self, state: &TableStorage) {
        self.clear();
        for (table_name, (columns, rows)) in state.tables.iter() {
            self.insert_table(table_name.clone(), columns.clone(), rows.clone());
        }
    }
}

impl Storage for TableStorage {
    type NewArgs = ();

    fn new(_args: ()) -> Self {
        Self::new()
    }

    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        row: Vec<Vec<DataValue>>,
    ) {
        self.tables.insert(table_name, (columns, row));
    }

    fn get_table(&mut self, table_name: &str) -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get_mut(table_name)
    }

    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
    }

    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) -> Result<usize, ReefDBError> {
        if let Some((_, rows)) = self.get_table(table_name) {
            rows.push(row);
            Ok(rows.len())
        } else {
            Err(ReefDBError::TableNotFound(table_name.to_string()))
        }
    }

    fn update_table(
        &mut self,
        table_name: &str,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        let mut updated_count = 0;
        if let Some((columns, rows)) = self.get_table(table_name) {
            for row in rows {
                let mut should_update = where_clause.as_ref().map_or(true, |(col, val)| {
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
        }
        updated_count
    }

    fn delete_table(
        &mut self,
        table_name: &str,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        let mut deleted_count = 0;
        if let Some((columns, rows)) = self.get_table(table_name) {
            let initial_len = rows.len();
            if let Some((col, val)) = where_clause {
                if let Some(col_idx) = columns.iter().position(|c| c.name == col) {
                    rows.retain(|row| &row[col_idx] != &val);
                    deleted_count = initial_len - rows.len();
                }
            } else {
                deleted_count = rows.len();
                rows.clear();
            }
        }
        deleted_count
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    fn remove_table(&mut self, table_name: &str) -> bool {
        self.tables.remove(table_name).is_some()
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
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clear(&mut self) {
        self.tables.clear();
    }

    fn get_all_tables(&self) -> &HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        &self.tables
    }
}

pub use disk::OnDiskStorage;
pub use memory::InMemoryStorage;
pub use mmap::MmapStorage;
