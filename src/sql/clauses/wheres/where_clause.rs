use nom::{
    bytes::complete::{tag, tag_no_case, take_till},
    character::complete::{alphanumeric1, multispace1, space1},
    sequence::delimited,
    IResult,
};

use crate::sql::{
    column::Column, column_def::column_name, data_value::DataValue, operators::op::Op,
};

use super::fts::FTSWhereClause;

#[derive(Debug, PartialEq)]
pub struct WhereClause {
    pub col_name: String,
    pub value: DataValue,
}
