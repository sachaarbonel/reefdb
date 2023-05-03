use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, multispace1},
    combinator::{opt, recognize},
    multi::{many0, separated_list0},
    sequence::{preceded, tuple},
    IResult,
};
use serde::{Deserialize, Serialize};

use super::{data_type::DataType, statements::constraints::Constraint};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

fn column_name(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    )))(input)
}

impl ColumnDef {
    pub fn parse(input: &str) -> IResult<&str, ColumnDef> {
        let (input, name) = column_name(input)?; // Use custom column_name() instead of alphanumeric1
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
    use crate::sql::statements::constraints::ForeignKeyConstraint;

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

    #[test]
    fn test_parse_column_def_with_foreign_key() {
        let input = "author_id INTEGER FOREIGN KEY (id) REFERENCES authors";
        let expected = ColumnDef {
            name: "author_id".to_string(),
            data_type: DataType::Integer,
            constraints: vec![Constraint::ForeignKey(ForeignKeyConstraint {
                table_name: "authors".to_string(),
                column_name: "id".to_string(),
            })],
        };
        let actual = ColumnDef::parse(input).unwrap().1;
        assert_eq!(expected, actual);
    }
}
