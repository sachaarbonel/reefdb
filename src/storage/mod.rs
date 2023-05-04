use crate::{sql::column_def::ColumnDef, sql::data_value::DataValue};

pub mod disk;
pub mod memory;

pub trait Storage {
    type NewArgs;
    fn new(args: Self::NewArgs) -> Self;
    fn insert_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDef>,
        row: Vec<Vec<DataValue>>,
    );

    fn get_table(&mut self, table_name: &str)
        -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)>;

    //non mutable
    fn get_table_ref(&self, table_name: &str)
        -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)>;
    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>);

    fn update_table(
        &mut self,
        table_name: &str,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<(String, DataValue)>,
    ) -> usize;

    fn delete_table(
        &mut self,
        table_name: &str,
        where_clause: Option<(String, DataValue)>,
    ) -> usize;

    fn table_exists(&self, table_name: &str) -> bool;

    fn get_schema(&mut self, table_name: &str) -> Option<&mut Vec<ColumnDef>> {
        self.get_table(table_name).map(|(schema, _)| schema)
    }

    // non mutable
    fn get_schema_ref(&self, table_name: &str) -> Option<&Vec<ColumnDef>> {
        self.get_table_ref(table_name).map(|(schema, _)| schema)
    }
}
