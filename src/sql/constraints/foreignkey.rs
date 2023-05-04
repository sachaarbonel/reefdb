use nom::{IResult, bytes::complete::{tag_no_case, tag}, character::complete::{multispace1, alphanumeric1}};
use serde::{Deserialize, Serialize};

use super::constraint::Constraint;


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


#[cfg(test)]
mod tests {
    #[test]
    fn parser_test() {
        use super::ForeignKeyConstraint;
        use crate::sql::constraints::constraint::Constraint;

        assert_eq!(
            ForeignKeyConstraint::parse("FOREIGN KEY (id) REFERENCES users"),
            Ok((
                "",
                Constraint::ForeignKey(ForeignKeyConstraint {
                    table_name: "users".to_string(),
                    column_name: "id".to_string(),
                })
            ))
        );
    }
}
