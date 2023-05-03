use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alphanumeric1, multispace1},
    combinator::map,
    IResult,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Constraint {
    NotNull,
    PrimaryKey,
    Unique,
    ForeignKey(ForeignKeyConstraint),
    // You can add more constraints here as needed.
}

impl Constraint {
    pub fn parse(input: &str) -> IResult<&str, Constraint> {
        alt((
            map(tag("NOT NULL"), |_| Constraint::NotNull),
            map(tag("PRIMARY KEY"), |_| Constraint::PrimaryKey),
            map(tag("UNIQUE"), |_| Constraint::Unique),
            ForeignKeyConstraint::parse,
        ))(input)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyConstraint {
    pub table_name: String,
    pub column_name: String,
}

impl ForeignKeyConstraint {
    pub fn parse(input: &str) -> IResult<&str, Constraint> {
        let (input, _) = tag_no_case("FOREIGN KEY")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("(")(input)?; // expect an opening parenthesis
        let (input, referenced_column) = alphanumeric1(input)?;
        let (input, _) = tag(")")(input)?; // expect a closing parenthesis
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("REFERENCES")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, referenced_table) = alphanumeric1(input)?;

        Ok((
            input,
            Constraint::ForeignKey(ForeignKeyConstraint {
                table_name: referenced_table.to_string(),
                column_name: referenced_column.to_string(),
            }),
        ))
    }
}
