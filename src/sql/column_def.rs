use nom::{
    character::complete::{alphanumeric1, multispace1},
    IResult, sequence::preceded, combinator::opt, multi::separated_list0,
};
use serde::{Deserialize, Serialize};

use super::{data_type::DataType, statements::constraints::Constraint};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

impl ColumnDef {
    pub fn parse(input: &str) -> IResult<&str, ColumnDef> {
        let (input, name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, data_type) = DataType::parse(input)?;
        let (input, constraints) = opt(preceded(
            multispace1,
            separated_list0(multispace1, Constraint::parse),
        ))(input)?;
        let constraints = constraints.unwrap_or_else(|| vec![]);

        Ok((
            input,
            ColumnDef {
                name: name.to_string(),
                data_type,
                constraints,
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
            constraints: vec![],
        };
        let actual = ColumnDef::parse(input).unwrap().1;
        assert_eq!(expected, actual);
    }



    #[test]
    fn test_parse_column_def_with_constraints() {
        let input = "id INTEGER PRIMARY KEY";
        let expected = ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            constraints: vec![Constraint::PrimaryKey],
        };
        let actual = ColumnDef::parse(input).unwrap().1;
        assert_eq!(expected, actual);
    }
}
