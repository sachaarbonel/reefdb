use std::collections::HashMap;

use super::Storage;


pub struct InMemoryStorage {
    tables: HashMap<String, Vec<Vec<String>>>,
}

impl Storage for InMemoryStorage {
    fn new() -> Self {
        InMemoryStorage {
            tables: HashMap::new(),
        }
    }

    fn insert(&mut self, table_name: String, row: Vec<Vec<String>>) {
        self.tables.insert(table_name, row);
    }

    fn get_table(&self, table_name: &str) -> Option<&Vec<Vec<String>>> {
        self.tables.get(table_name)
    }

    fn get_mut(&mut self, table_name: &str) -> Option<&mut Vec<Vec<String>>> {
        self.tables.get_mut(table_name)
    }

    fn get(&self, table_name: &str) -> Option<&Vec<Vec<String>>> {
        self.tables.get(table_name)
    }

    fn contains_key(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
}
