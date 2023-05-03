use nom::{
    character::complete::{alphanumeric1, multispace1},
    IResult,
};
use serde::{Deserialize, Serialize};

use super::data_type::DataType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnDef {
    pub fn parse(input: &str) -> IResult<&str, ColumnDef> {
        let (input, name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, data_type) = DataType::parse(input)?;

        Ok((
            input,
            ColumnDef {
                name: name.to_string(),
                data_type,
            },
        ))
    }
}
