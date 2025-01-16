use crate::sql::{
    clauses::{
        join_clause::{JoinClause, TableReference},
        wheres::where_type::{WhereType, parse_where_clause},
    },
    column::Column,
    column_value_pair::identifier,
    statements::Statement,
};

use nom::{
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1, alphanumeric1},
    combinator::{map, opt},
    multi::{many0, separated_list0},
    sequence::{preceded, terminated, tuple, delimited},
    branch::alt,
    IResult,
};

#[derive(Debug, PartialEq, Clone)]
pub enum SelectStatement {
    FromTable(TableReference, Vec<Column>, Option<WhereType>, Vec<JoinClause>),
}

impl SelectStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = delimited(
            multispace0,
            tag_no_case("SELECT"),
            multispace1
        )(input)?;
        let (input, columns) = delimited(
            multispace0,
            parse_column_list,
            multispace0
        )(input)?;
        let (input, _) = delimited(
            multispace0,
            tag_no_case("FROM"),
            multispace0
        )(input)?;
        let (input, table_name) = delimited(
            multispace0,
            identifier,
            multispace0
        )(input)?;
        let (input, alias) = opt(preceded(
            delimited(
                multispace0,
                tag_no_case("AS"),
                multispace1
            ),
            identifier
        ))(input)?;

        let table_ref = TableReference {
            name: table_name.to_string(),
            alias: alias.map(|a| a.to_string()),
        };

        let (input, joins) = many0(delimited(
            multispace0,
            JoinClause::parse,
            multispace0
        ))(input)?;

        let (input, where_clause) = opt(preceded(
            multispace0,
            parse_where_clause
        ))(input)?;

        let (input, _) = multispace0(input)?;

        if !input.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof
            )));
        }

        Ok((
            input,
            Statement::Select(SelectStatement::FromTable(
                table_ref,
                columns,
                where_clause,
                joins,
            )),
        ))
    }

    pub fn get_tables(&self) -> Vec<&str> {
        match self {
            SelectStatement::FromTable(table_ref, _, _, joins) => {
                let mut tables = vec![table_ref.name.as_str()];
                for join in joins {
                    tables.push(&join.table_ref.name);
                }
                tables
            }
        }
    }
}

fn parse_column_list(input: &str) -> IResult<&str, Vec<Column>> {
    alt((
        // Handle single * wildcard with optional whitespace
        map(
            delimited(
                multispace0,
                tag("*"),
                multispace0
            ),
            |_| vec![Column {
                table: None,
                name: "*".to_string(),
            }]
        ),
        // Handle comma-separated list of columns
        separated_list0(
            terminated(tag(","), multispace0),
            map(
                tuple((
                    opt(terminated(identifier, tag("."))),
                    identifier
                )),
                |(table, name)| Column {
                    table: table.map(|t| t.to_string()),
                    name: name.to_string(),
                }
            )
        )
    ))(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{
        clauses::join_clause::JoinType,
        column_value_pair::ColumnValuePair,
        column::Column,
        statements::Statement,
    };

    #[test]
    fn parse_select_test() {
        let input = "SELECT name FROM users";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement::FromTable(
                TableReference {
                    name: "users".to_string(),
                    alias: None,
                },
                vec![Column {
                    table: None,
                    name: "name".to_string(),
                }],
                None,
                vec![]
            ))
        );
    }

    #[test]
    fn parse_select_with_alias_test() {
        let input = "SELECT u.name FROM users AS u";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement::FromTable(
                TableReference {
                    name: "users".to_string(),
                    alias: Some("u".to_string()),
                },
                vec![Column {
                    table: Some("u".to_string()),
                    name: "name".to_string(),
                }],
                None,
                vec![]
            ))
        );
    }

    #[test]
    fn parse_select_join_test() {
        let input = "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id";
        let result = SelectStatement::parse(input);
        assert_eq!(result.is_ok(), true);
        let (_input, statement) = result.unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement::FromTable(
                TableReference {
                    name: "users".to_string(),
                    alias: None,
                },
                vec![
                    Column {
                        table: Some("users".to_string()),
                        name: "name".to_string(),
                    },
                    Column {
                        table: Some("orders".to_string()),
                        name: "item".to_string(),
                    }
                ],
                None,
                vec![JoinClause {
                    join_type: JoinType::Inner,
                    table_ref: TableReference {
                        name: "orders".to_string(),
                        alias: None,
                    },
                    on: (
                        ColumnValuePair::new("id", "users"),
                        ColumnValuePair::new("user_id", "orders")
                    )
                }]
            ))
        );
    }

    // add test for  "SELECT * FROM test_types WHERE int_col = 123",
        // "SELECT * FROM test_types WHERE text_col = 'Hello World'",
        // "SELECT * FROM test_types WHERE bool_col = true",
        // "SELECT * FROM test_types WHERE float_col > 45.0",
        // "SELECT * FROM test_types WHERE date_col = '2024-03-14'",
        // "SELECT * FROM test_types WHERE timestamp_col = '2024-03-14 12:34:56'",

        #[test]
        fn parse_select_star_test2() {
            let input = "SELECT * FROM test_types";
            let result = SelectStatement::parse(input);
            assert_eq!(result.is_ok(), true);
            let (_input, statement) = result.unwrap();
            assert_eq!(
                statement,
                Statement::Select(SelectStatement::FromTable(
                    TableReference {
                        name: "test_types".to_string(),
                        alias: None,
                    },
                    vec![Column {
                        table: None,
                        name: "*".to_string(),
                    }],
                    None,
                    vec![]
                ))
            );
        }

    

    #[test]
    fn parse_select_star_test() {
        let input = "SELECT * FROM users";
        let result = SelectStatement::parse(input);
        assert_eq!(result.is_ok(), true);
        let (_input, statement) = result.unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement::FromTable(
                TableReference {
                    name: "users".to_string(),
                    alias: None,
                },
                vec![Column {
                    table: None,
                    name: "*".to_string(),
                }],
                None,
                vec![]
            ))
        );
    }
}
