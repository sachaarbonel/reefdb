use crate::sql::column_def::ColumnDef;
use std::collections::HashMap;
use std::any::Any;

use super::Storage;
use crate::sql::data_value::DataValue;

#[derive(Clone)]
pub struct InMemoryStorage {
    tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
}

impl Storage for InMemoryStorage {
    type NewArgs = ();
    fn new(_args: ()) -> Self {
        InMemoryStorage {
            tables: HashMap::new(),
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

    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) -> usize {
        if let Some((_, rows)) = self.get_table(table_name) {
            // Add the new row to the table
            rows.push(row);

            // Return the rowid (1-based index)
            rows.len()
        } else {
            // Return an error value if the table doesn't exist
            0
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
        last_updated_row_id
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

    fn as_any(&self) -> &dyn Any {
        self
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
