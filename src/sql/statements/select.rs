use nom::IResult;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, terminated, tuple},
};
use crate::sql::{
    clauses::{
        join_clause::JoinClause,
        wheres::where_type::{WhereType, parse_where_clause},
    },
    column::{Column, ColumnType},
    data_value::DataValue,
    table_reference::TableReference,
    operators::op::Op,
};
use crate::sql::statements::Statement;

#[derive(Debug, PartialEq, Clone)]
pub enum SelectStatement {
    FromTable(TableReference, Vec<Column>, Option<WhereType>, Vec<JoinClause>),
}

impl SelectStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("SELECT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, columns) = parse_column_list(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_ref) = parse_table_reference(input)?;
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
        Ok((input, Statement::Select(SelectStatement::FromTable(
            table_ref,
            columns,
            where_clause,
            joins,
        ))))
    }
}

fn parse_table_reference(input: &str) -> IResult<&str, TableReference> {
    let (input, name) = identifier(input)?;
    let (input, alias) = opt(preceded(
        delimited(multispace0, tag_no_case("AS"), multispace1),
        identifier
    ))(input)?;
    Ok((input, TableReference {
        name: name.to_string(),
        alias: alias.map(|a| a.to_string()),
    }))
}

fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(
        tuple((
            alpha1,
            many0(alt((alphanumeric1, tag("_"))))
        ))
    )(input)
}

fn parse_column_list(input: &str) -> IResult<&str, Vec<Column>> {
    alt((
        // Handle SELECT *
        map(
            tag("*"),
            |_| vec![Column {
                table: None,
                name: "*".to_string(),
                column_type: ColumnType::Wildcard,
            }]
        ),
        // Handle comma-separated list of columns
        separated_list1(
            delimited(multispace0, tag(","), multispace0),
            alt((
                // Handle function calls with optional alias
                map(
                    tuple((
                        DataValue::parse_function,
                        opt(preceded(
                            delimited(multispace0, tag_no_case("as"), multispace1),
                            identifier
                        ))
                    )),
                    |(func, alias)| match func {
                        DataValue::Function { name, args } => {
                            let alias_name = alias.map(|a| a.to_string()).unwrap_or(name.clone());
                            Column {
                                table: None,
                                name: alias_name,
                                column_type: ColumnType::Function(
                                    name, 
                                    args.into_iter()
                                        .map(|arg| match &arg {
                                            DataValue::Text(s) => DataValue::Text(s.clone()),
                                            DataValue::Function { name, args } => DataValue::Function {
                                                name: name.clone(),
                                                args: args.clone(),
                                            },
                                            _ => DataValue::Text(arg.to_string()),
                                        })
                                        .collect()
                                ),
                            }
                        },
                        _ => panic!("Expected function"),
                    }
                ),
                // Handle regular columns with optional table prefix
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
            ))
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

    //ts rank test
    // //"SELECT id,title,ts_rank(to_tsvector(content),to_tsquery('rust')) as rank FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')"
    #[test]
    fn parse_select_ts_rank_test() {
        let input = "SELECT id,title,ts_rank(to_tsvector(content),to_tsquery('rust')) as rank FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        match statement {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, joins)) => {
                assert_eq!(table_ref.name, "articles");
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[1].name, "title");
                assert_eq!(columns[2].name, "rank");
            }
            _ => panic!("Expected Select statement with ts_rank"),
        }
    }

   
    #[test]
    fn parse_select_join_test2() {
        let input = "SELECT authors.name, books.title, books.year FROM authors INNER JOIN books ON authors.id = books.author_id WHERE books.year > 2020";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        match statement {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, joins)) => {
                    assert_eq!(table_ref.name, "authors");
                    assert_eq!(columns.len(), 3);
                    assert_eq!(columns[0].name, "name");
                    assert_eq!(columns[1].name, "title");
                    assert_eq!(columns[2].name, "year");
                    assert_eq!(joins.len(), 1);
                    assert_eq!(joins[0].join_type, JoinType::Inner);
                    assert_eq!(joins[0].table_ref.name, "books");
                    assert_eq!(joins[0].on.0.column_name, "id");
                    assert_eq!(joins[0].on.1.column_name, "author_id");
                    match where_clause.unwrap() {
                        WhereType::Regular(where_clause) => {
                            assert_eq!(where_clause.col_name, "year");
                            assert_eq!(where_clause.operator, Op::GreaterThan);
                            assert_eq!(where_clause.value, DataValue::Integer(2020));
                        }
                        _ => panic!("Expected Select statement with where clause"),
                    }
            }
            _ => panic!("Expected Select statement with join"),
        }
    }

    #[test]
    fn parse_select_where_test() {
        let input = "SELECT * FROM books WHERE year > 2020";
        let result = SelectStatement::parse(input);
        let (_input, statement) = result.unwrap();
        match statement {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, joins)) => {
                assert_eq!(table_ref.name, "books");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "*");
              //match regular where clause
              match where_clause.unwrap() {
                WhereType::Regular(where_clause) => {
                    assert_eq!(where_clause.col_name, "year");
                    assert_eq!(where_clause.operator, Op::GreaterThan);
                    assert_eq!(where_clause.value, DataValue::Integer(2020));
                }
                _ => panic!("Expected Select statement with where clause"),
            }
            }
            _ => panic!("Expected Select statement with where clause"),
        }
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
