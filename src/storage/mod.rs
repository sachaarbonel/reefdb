
pub mod memory;
pub trait Storage {
    fn new() -> Self;
    fn insert(&mut self, table_name: String, row: Vec<Vec<String>>);
    fn get_table(&self, table_name: &str) -> Option<&Vec<Vec<String>>>;
    fn get(&self, table_name: &str) -> Option<&Vec<Vec<String>>>;
    fn get_mut(&mut self, table_name: &str) -> Option<&mut Vec<Vec<String>>>;
    // contains_key
    fn contains_key(&self, table_name: &str) -> bool;
}
