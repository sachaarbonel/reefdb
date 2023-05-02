use crate::{ColumnDef, DataValue};


pub mod memory;

pub trait Storage {
    fn new() -> Self;
    fn insert(&mut self, table_name: String, columns: Vec<ColumnDef>, row: Vec<Vec<DataValue>>);
    fn get_table(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)>;
    fn get(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)>;
    fn get_mut(&mut self, table_name: &str)
        -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)>;
    // contains_key
    fn contains_key(&self, table_name: &str) -> bool;
}