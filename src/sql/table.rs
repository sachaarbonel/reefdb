use crate::sql::{column_def::ColumnDef, data_value::DataValue};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Table {
    pub data: (Vec<ColumnDef>, Vec<Vec<DataValue>>),
}

impl Table {
    pub fn new(schema: Vec<ColumnDef>) -> Self {
        Table {
            data: (schema, Vec::new()),
        }
    }

    pub fn insert_row(&mut self, row: Vec<DataValue>) -> usize {
        let row_id = self.data.1.len();
        self.data.1.push(row);
        row_id
    }

    pub fn get_schema(&self) -> &Vec<ColumnDef> {
        &self.data.0
    }

    pub fn get_rows(&self) -> &Vec<Vec<DataValue>> {
        &self.data.1
    }
} 