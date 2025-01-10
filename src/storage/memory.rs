use crate::sql::column_def::ColumnDef;
use std::collections::HashMap;
use std::any::Any;

use super::Storage;
use crate::sql::data_value::DataValue;
use crate::sql::data_type::DataType;
use crate::error::ReefDBError;
use crate::sql::constraints::constraint::Constraint;
use crate::indexes::{IndexManager, DefaultIndexManager};

#[derive(Clone)]
pub struct InMemoryStorage {
    tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
    index_manager: DefaultIndexManager,
}

impl Storage for InMemoryStorage {
    type NewArgs = ();
    fn new(_args: ()) -> Self {
        InMemoryStorage {
            tables: HashMap::new(),
            index_manager: DefaultIndexManager::new(),
        }
    }

    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        row: Vec<Vec<DataValue>>,
    ) {
        self.tables.insert(table_name, (columns, row));
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

            // Add the new row to the table
            rows.push(row);

            // Return the rowid (1-based index)
            Ok(rows.len())
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
        let mut updated_count = 0;

        for row in rows.iter_mut() {
            let matches_where = if let Some((column, value)) = &where_clause {
                let column_idx = schema.iter().position(|c| c.name == *column).unwrap();
                row[column_idx] == *value
            } else {
                true
            };

            if matches_where {
                for (col_name, new_value) in &updates {
                    if let Some(col_idx) = schema.iter().position(|c| c.name == *col_name) {
                        row[col_idx] = new_value.clone();
                    }
                }
                updated_count += 1;
            }
        }

        updated_count
    }

    fn delete_table(
        &mut self,
        table_name: &str,
        where_clause: Option<(String, DataValue)>,
    ) -> usize {
        if let Some((schema, rows)) = self.get_table(table_name) {
            let initial_len = rows.len();
            
            if let Some((column, value)) = where_clause {
                let column_idx = schema.iter().position(|c| c.name == column).unwrap();
                rows.retain(|row| row[column_idx] != value);
                initial_len - rows.len()
            } else {
                let count = rows.len();
                rows.clear();
                count
            }
        } else {
            0
        }
    }

    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&mut self) {
        self.tables.clear();
    }

    fn get_all_tables(&self) -> &HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        &self.tables
    }
}

impl IndexManager for InMemoryStorage {
    fn create_index(&mut self, table: &str, column: &str, index_type: crate::indexes::IndexType) {
        self.index_manager.create_index(table, column, index_type);
    }

    fn drop_index(&mut self, table: &str, column: &str) {
        self.index_manager.drop_index(table, column);
    }

    fn get_index(&self, table: &str, column: &str) -> Option<&crate::indexes::IndexType> {
        self.index_manager.get_index(table, column)
    }

    fn update_index(&mut self, table: &str, column: &str, old_value: Vec<u8>, new_value: Vec<u8>, row_id: usize) {
        self.index_manager.update_index(table, column, old_value, new_value, row_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::{constraints::constraint::Constraint, data_type::DataType};

    #[test]
    fn test() {
        use super::*;
        use crate::sql::column_def::ColumnDef;
        use crate::sql::data_value::DataValue;
        let mut storage = InMemoryStorage::new(());
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
