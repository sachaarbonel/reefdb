use crate::sql::data_value::DataValue;


#[derive(PartialEq, Debug)]
pub enum ToyDBResult {
    Select(Vec<(usize, Vec<DataValue>)>),
    Insert(usize),
    CreateTable,
    Update(usize),
    Delete(usize),
}
