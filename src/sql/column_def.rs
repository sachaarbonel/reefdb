use nom::{
    character::complete::{alphanumeric1, multispace1},
    IResult,
};
use serde::{Deserialize, Serialize};

use super::data_type::DataType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

//test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_column_def() {
        let input = "id INTEGER";
        let expected = ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
        };
        let actual = ColumnDef::parse(input).unwrap().1;
        assert_eq!(expected, actual);
    }
}
