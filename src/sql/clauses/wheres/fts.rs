use nom::{
    bytes::complete::{tag, tag_no_case, take_till},
    character::complete::space1,
    sequence::delimited,
    IResult,
};

use crate::sql::{column::Column, data_value::DataValue, operators::op::Op};

#[derive(Debug, PartialEq)]
pub struct FTSWhereClause {
    pub col: Column,
    pub query: String,
}
