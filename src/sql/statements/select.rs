use crate::sql::{
    clauses::{
        join_clause::JoinClause,
        wheres::where_type::{WhereType, parse_where_clause},
    },
    column::Column,
    column_value_pair::identifier,
    statements::Statement,
};

use nom::{
    bytes::complete::tag,
    character::complete::{multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0},
    sequence::{terminated, tuple},
    branch::alt,
    IResult,
};

#[derive(Debug, PartialEq, Clone)]
pub enum SelectStatement {
    FromTable(String, Vec<Column>, Option<WhereType>, Vec<JoinClause>),
}

impl SelectStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("SELECT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, columns) = parse_column_list(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = identifier(input)?;

        let (input, _) = opt(multispace1)(input)?;
        let (input, joins) = many0(JoinClause::parse)(input)?;

        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(parse_where_clause)(input)?;

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
                table_name.to_string(),
                columns,
                where_clause,
                joins,
            )),
        ))
    }

    pub fn get_tables(&self) -> Vec<&str> {
        match self {
            SelectStatement::FromTable(table_name, _, _, joins) => {
                let mut tables = vec![table_name.as_str()];
                for join in joins {
                    tables.push(&join.table_name);
                }
                tables
            }
        }
    }
}

fn parse_column_list(input: &str) -> IResult<&str, Vec<Column>> {
    separated_list0(
        terminated(tag(","), multispace0),
        alt((
            // Handle * wildcard
            map(tag("*"), |_| Column {
                table: None,
                name: "*".to_string(),
            }),
            // Handle existing column parsing
            map(
                recognize(tuple((
                    opt(terminated(identifier, tag("."))),
                    identifier,
                ))),
                |column_str: &str| {
                    let parts: Vec<_> = column_str.split('.').collect();

                    if parts.len() == 2 {
                        Column {
                            table: Some(parts[0].to_string()),
                            name: parts[1].to_string(),
                        }
                    } else {
                        Column {
                            table: None,
                            name: parts[0].to_string(),
                        }
                    }
                },
            ),
        )),
    )(input)
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
                "users".to_string(),
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
    fn parse_select_join_test() {
        let input = "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id";
        let result = SelectStatement::parse(input);
        assert_eq!(result.is_ok(), true);
        let (_input, statement) = result.unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement::FromTable(
                "users".to_string(),
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
                    table_name: "orders".to_string(),
                    on: (
                        ColumnValuePair::new("id", "users"),
                        ColumnValuePair::new("user_id", "orders")
                    )
                }]
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
                "users".to_string(),
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
