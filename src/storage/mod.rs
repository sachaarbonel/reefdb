use crate::ColumnDef;


pub mod memory;

pub trait Storage {
    fn new() -> Self;
    fn insert(&mut self, table_name: String, columns: Vec<ColumnDef>, row: Vec<Vec<String>>);
    fn get_table(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<String>>)>;
    fn get(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<String>>)>;
    fn get_mut(&mut self, table_name: &str)
        -> Option<&mut (Vec<ColumnDef>, Vec<Vec<String>>)>;
    // contains_key
    fn contains_key(&self, table_name: &str) -> bool;
}