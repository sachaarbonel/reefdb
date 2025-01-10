use std::collections::HashSet;

pub trait Search {
    type NewArgs: Default;
    fn new(args: Self::NewArgs) -> Self;
    fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize>;

    fn add_column(&mut self, table: &str, column: &str);
    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str);
    fn remove_document(&mut self, table: &str, column: &str, row_id: usize);
    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str);
}