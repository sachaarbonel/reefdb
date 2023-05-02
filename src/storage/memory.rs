use std::collections::HashMap;

use crate::ColumnDef;

use super::Storage;
use crate::DataValue;


pub struct InMemoryStorage {
    tables: HashMap<String, (Vec<ColumnDef>, Vec<Vec<DataValue>>)>,
}


impl Storage for InMemoryStorage {
    type NewArgs = ();
    fn new(_args :() ) -> Self {
        InMemoryStorage {
            tables: HashMap::new(),
        }
    }

    fn insert(&mut self, table_name: String, columns: Vec<ColumnDef>, row: Vec<Vec<DataValue>>) {
        self.tables.insert(table_name, (columns, row));
    }

    fn get_table(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
    }

    fn get_mut(
        &mut self,
        table_name: &str,
    ) -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get_mut(table_name)
    }

    fn get(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name)
    }

    fn contains_key(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
}
