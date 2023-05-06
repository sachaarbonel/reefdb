#[derive(Debug, PartialEq)]
pub enum ReefDBError {
    TableNotFound(String),
    ColumnNotFound(String),
    ParseError(String),
    Other(String),
}
