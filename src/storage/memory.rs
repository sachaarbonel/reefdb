use crate::sql::column_def::ColumnDef;
use std::collections::HashMap;

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

            // Return the rowid (index of the newly added row)
            rows.len() - 1
        } else {
            // Return an error value if the table doesn't exist
            usize::MAX
        }
    }

    fn update_table(
        &mut self,
        table_name: &str,
        updates: Vec<(String, DataValue)>,
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

            for (column, value) in &updates {
                let column_idx = schema.iter().position(|c| c.name == *column).unwrap();
                row[column_idx] = value.clone();
            }
            idx += 1;
        }
        idx
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
        idx
    }

    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
    }

    fn remove_table(&mut self, table_name: &str) -> bool {
        self.tables.remove(table_name).is_some()
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
