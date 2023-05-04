use crate::sql::clauses::{join_clause::JoinClause, where_clause::WhereClause};

use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, multispace1, space1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0},
    sequence::{preceded, terminated, tuple},
    IResult,
};

use super::Statement;
#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum SelectStatement {
    FromTable(String, Vec<Column>, Option<WhereClause>, Vec<JoinClause>),
}

impl SelectStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("SELECT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, columns) = parse_column_list(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;

        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(WhereClause::parse)(input)?;

        let (input, joins) = many0(JoinClause::parse)(input)?;

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
}

fn parse_column_list(input: &str) -> IResult<&str, Vec<Column>> {
    separated_list0(
        terminated(tag(","), multispace0),
        map(
            recognize(tuple((
                opt(terminated(alphanumeric1, tag("."))),
                alphanumeric1,
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
    )(input)
}

#[cfg(test)]
mod tests {
    
    use crate::sql::{clauses::join_clause::JoinType, column_value_pair::ColumnValuePair};

    #[test]
    fn parse_select_test() {
        use super::*;
        let input = "SELECT name FROM table1";
        let result = SelectStatement::parse(input);
        assert_eq!(result.is_ok(), true);
        let (input, statement) = result.unwrap();
        assert_eq!(input, "");
        assert_eq!(
            statement,
            Statement::Select(SelectStatement::FromTable(
                "table1".to_string(),
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
        use super::*;
        let input = "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id";
        let result = SelectStatement::parse(input);
        // assert_eq!(result.is_ok(), true);
        let (input, statement) = result.unwrap();
        assert_eq!(input, "");
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
}
