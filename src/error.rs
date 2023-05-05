
#[derive(Debug, PartialEq)]
pub enum ToyDBError {
    TableNotFound(String),
    ColumnNotFound(String),
    ParseError(String),
    Other(String),
}