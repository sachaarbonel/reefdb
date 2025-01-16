use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    combinator::{map, opt},
    multi::{many0, separated_list0},
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

use crate::sql::{
    clauses::{
        join_clause::{JoinClause, JoinType},
        wheres::where_type::{parse_where_clause, WhereType},
    },
    column::{Column, ColumnType},
    column_value_pair::identifier,
    data_value::DataValue,
    operators::op::Op,
    statements::Statement,
    table_reference::TableReference,
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

        let (input, where_clause) = opt(delimited(
            multispace0,
            parse_where_clause,
            multispace0
        ))(input)?;

        Ok((input, Statement::Select(SelectStatement::FromTable(
            table_ref,
            columns,
            where_clause,
            joins,
        ))))
    }
}

fn parse_column_list(input: &str) -> IResult<&str, Vec<Column>> {
    alt((
        // Handle SELECT *
        map(
            delimited(
                multispace0,
                tag("*"),
                multispace0
            ),
            |_| vec![Column {
                table: None,
                name: "*".to_string(),
                column_type: ColumnType::Wildcard,
            }]
        ),
        // Handle comma-separated list of columns
        separated_list0(
            delimited(multispace0, tag(","), multispace0),
            map(
                tuple((
                    opt(terminated(identifier, tag("."))),
                    identifier
                )),
                |(table, name)| Column {
                    table: table.map(|t| t.to_string()),
                    name: name.to_string(),
                    column_type: ColumnType::Regular(name.to_string()),
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
                    column_type: ColumnType::Regular("name".to_string()),
                }],
                None,
                vec![]
            ))
        );
    }

    #[test]
    fn parse_select_with_where_test() {
        let input = "SELECT name FROM users WHERE id = 1";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        match statement {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, Some(WhereType::Regular(where_clause)), joins)) => {
                assert_eq!(table_ref.name, "users");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "name");
                assert_eq!(where_clause.col_name, "id");
                assert_eq!(where_clause.operator, Op::Equal);
                assert_eq!(where_clause.value, DataValue::Integer(1));
                assert!(joins.is_empty());
            }
            _ => panic!("Expected Select statement with where clause"),
        }
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
                    column_type: ColumnType::Regular("name".to_string()),
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
                    column_type: ColumnType::Wildcard,
                }],
                None,
                vec![]
            ))
        );
    }

    #[test]
    fn parse_select_join_test() {
        let input = "SELECT u.name, p.title FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        match statement {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, joins)) => {
                assert_eq!(table_ref.name, "users");
                assert_eq!(table_ref.alias, Some("u".to_string()));
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].table, Some("u".to_string()));
                assert_eq!(columns[0].name, "name");
                assert_eq!(columns[1].table, Some("p".to_string()));
                assert_eq!(columns[1].name, "title");
                assert!(where_clause.is_none());
                assert_eq!(joins.len(), 1);
                assert_eq!(joins[0].join_type, JoinType::Inner);
                assert_eq!(joins[0].table_ref.name, "posts");
                assert_eq!(joins[0].table_ref.alias, Some("p".to_string()));
            }
            _ => panic!("Expected Select statement with join"),
        }
    }
}
