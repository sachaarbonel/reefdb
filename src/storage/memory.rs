use std::collections::HashMap;

use crate::ColumnDef;

use super::Storage;
use crate::DataValue;

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

    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) {
        let (_, rows) = self.get_table(table_name).unwrap();
        rows.push(row);
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
}
