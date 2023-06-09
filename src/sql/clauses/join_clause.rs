use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    combinator::value,
    IResult,
};
use serde::{Deserialize, Serialize};

use crate::sql::{column_def::table_name, column_value_pair::ColumnValuePair};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table_name: String,
    pub on: (ColumnValuePair, ColumnValuePair),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    // Add other join types if needed
}

fn join_type(input: &str) -> IResult<&str, JoinType> {
    alt((
        value(JoinType::Inner, tag_no_case("INNER")),
        value(JoinType::Left, tag_no_case("LEFT")),
        value(JoinType::Right, tag_no_case("RIGHT")),
        value(JoinType::Full, tag_no_case("FULL")),
        // Add other join types if needed
    ))(input)
}

impl JoinClause {
    pub fn new(
        join_type: JoinType,
        table_name: &str,
        on: (ColumnValuePair, ColumnValuePair),
    ) -> JoinClause {
        JoinClause {
            join_type,
            table_name: table_name.to_owned(),
            on: on,
        }
    }

    pub fn parse(input: &str) -> IResult<&str, JoinClause> {
        let (input, join_type) = join_type(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("JOIN")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = table_name(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("ON")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, col1) = ColumnValuePair::parse(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = tag("=")(input)?;
        let (input, _) = multispace0(input)?;
        let (input, col2) = ColumnValuePair::parse(input)?;

        Ok((
            input,
            JoinClause {
                join_type,
                table_name: table_name.to_owned(),
                on: (col1, col2),
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn join_parse_test() {
        use super::*;
        let (input, join) =
            JoinClause::parse("INNER JOIN table1 ON table1.id = table2.id").unwrap();
        assert_eq!(input, "");
        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(join.table_name, "table1");
        assert_eq!(
            join.on.0,
            ColumnValuePair {
                column_name: "id".to_owned(),
                table_name: "table1".to_owned()
            }
        );

        assert_eq!(
            join.on.1,
            ColumnValuePair {
                column_name: "id".to_owned(),
                table_name: "table2".to_owned()
            }
        );
    }

    #[test]

    fn join_parse_2_test() {
        use super::*;
        let (input, join) =
            JoinClause::parse("INNER JOIN orders ON users.id = orders.user_id").unwrap();
        assert_eq!(input, "");
        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(join.table_name, "orders");
        assert_eq!(
            join.on.0,
            ColumnValuePair {
                column_name: "id".to_owned(),
                table_name: "users".to_owned()
            }
        );

        assert_eq!(
            join.on.1,
            ColumnValuePair {
                column_name: "user_id".to_owned(),
                table_name: "orders".to_owned()
            }
        );
    }
}
