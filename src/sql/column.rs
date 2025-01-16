use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{multispace0},
    combinator::{map, verify, fail},
    multi::separated_list0,
    sequence::{delimited, terminated, tuple},
    error::{Error, ErrorKind},
};

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnType {
    Regular(String),
    Wildcard,
    Function {
        name: String,
        args: Vec<Column>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
    pub column_type: ColumnType,
}

fn identifier(input: &str) -> IResult<&str, &str> {
    verify(
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        |s: &str| !s.contains(' ') && !s.is_empty() && s.chars().next().map_or(false, |c| c.is_alphabetic()),
    )(input)
}

impl Column {
    pub fn parse(input: &str) -> IResult<&str, Column> {
        if input.trim().is_empty() {
            return Err(nom::Err::Error(Error::new(input, ErrorKind::Fail)));
        }

        let (input, _) = multispace0(input)?;

        let (input, result) = alt((
            // Parse function
            map(
                tuple((
                    identifier,
                    delimited(
                        multispace0,
                        tag("("),
                        multispace0,
                    ),
                    separated_list0(
                        delimited(multispace0, tag(","), multispace0),
                        Column::parse,
                    ),
                    delimited(
                        multispace0,
                        tag(")"),
                        multispace0,
                    ),
                )),
                |(func_name, _, args, _)| Column {
                    name: func_name.to_string(),
                    table: None,
                    column_type: ColumnType::Function {
                        name: func_name.to_string(),
                        args,
                    },
                },
            ),
            // Parse table.column
            map(
                tuple((
                    identifier,
                    delimited(multispace0, tag("."), multispace0),
                    identifier,
                )),
                |(table, _, column)| Column {
                    name: column.to_string(),
                    table: Some(table.to_string()),
                    column_type: ColumnType::Regular(column.to_string()),
                },
            ),
            // Parse wildcard
            map(
                tag("*"),
                |_| Column {
                    name: "*".to_string(),
                    table: None,
                    column_type: ColumnType::Wildcard,
                },
            ),
            // Parse just column
            map(
                verify(
                    identifier,
                    |s: &str| !s.contains(' '),
                ),
                |column| Column {
                    name: column.to_string(),
                    table: None,
                    column_type: ColumnType::Regular(column.to_string()),
                },
            ),
        ))(input)?;

        let (input, _) = multispace0(input)?;
        Ok((input, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::combinator::all_consuming;

    #[test]
    fn test_parse_regular_column() {
        let input = "id";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "id");
        assert_eq!(column.table, None);
        assert_eq!(column.column_type, ColumnType::Regular("id".to_string()));
    }

    #[test]
    fn test_parse_table_column() {
        let input = "users.id";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "id");
        assert_eq!(column.table, Some("users".to_string()));
        assert_eq!(column.column_type, ColumnType::Regular("id".to_string()));
    }

    #[test]
    fn test_parse_wildcard() {
        let input = "*";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "*");
        assert_eq!(column.table, None);
        assert_eq!(column.column_type, ColumnType::Wildcard);
    }

    #[test]
    fn test_parse_function() {
        let input = "count(*)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "count");
        assert_eq!(column.table, None);
        match column.column_type {
            ColumnType::Function { name, args } => {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].name, "*");
                assert_eq!(args[0].table, None);
                assert_eq!(args[0].column_type, ColumnType::Wildcard);
            }
            _ => panic!("Expected Function type"),
        }
    }

    #[test]
    fn test_parse_function_with_multiple_args() {
        let input = "concat(first_name, last_name)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "concat");
        assert_eq!(column.table, None);
        match column.column_type {
            ColumnType::Function { name, args } => {
                assert_eq!(name, "concat");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].name, "first_name");
                assert_eq!(args[0].table, None);
                assert_eq!(args[0].column_type, ColumnType::Regular("first_name".to_string()));
                assert_eq!(args[1].name, "last_name");
                assert_eq!(args[1].table, None);
                assert_eq!(args[1].column_type, ColumnType::Regular("last_name".to_string()));
            }
            _ => panic!("Expected Function type"),
        }
    }

    #[test]
    fn test_parse_function_with_table_column() {
        let input = "count(users.id)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "count");
        assert_eq!(column.table, None);
        match column.column_type {
            ColumnType::Function { name, args } => {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].name, "id");
                assert_eq!(args[0].table, Some("users".to_string()));
                assert_eq!(args[0].column_type, ColumnType::Regular("id".to_string()));
            }
            _ => panic!("Expected Function type"),
        }
    }

    #[test]
    fn test_parse_invalid_input() {
        assert!(all_consuming(Column::parse)("").is_err());
        assert!(all_consuming(Column::parse)("invalid input").is_err());
        assert!(all_consuming(Column::parse)("count(").is_err());
        assert!(all_consuming(Column::parse)("count)").is_err());
        assert!(all_consuming(Column::parse)("users.").is_err());
        assert!(all_consuming(Column::parse)(".id").is_err());
    }

    #[test]
    fn test_parse_with_whitespace() {
        // Function with whitespace
        let input = "count ( * )";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "count");
        assert_eq!(column.table, None);
        match column.column_type {
            ColumnType::Function { name, args } => {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].name, "*");
                assert_eq!(args[0].table, None);
                assert_eq!(args[0].column_type, ColumnType::Wildcard);
            }
            _ => panic!("Expected Function type"),
        }

        // Table.column with whitespace
        let input = "users . id";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "id");
        assert_eq!(column.table, Some("users".to_string()));
        assert_eq!(column.column_type, ColumnType::Regular("id".to_string()));

        // Function with multiple args and whitespace
        let input = "concat ( first_name ,  last_name )";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "concat");
        assert_eq!(column.table, None);
        match column.column_type {
            ColumnType::Function { name, args } => {
                assert_eq!(name, "concat");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].name, "first_name");
                assert_eq!(args[0].table, None);
                assert_eq!(args[0].column_type, ColumnType::Regular("first_name".to_string()));
                assert_eq!(args[1].name, "last_name");
                assert_eq!(args[1].table, None);
                assert_eq!(args[1].column_type, ColumnType::Regular("last_name".to_string()));
            }
            _ => panic!("Expected Function type"),
        }

        // Leading and trailing whitespace
        let input = "  id  ";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "id");
        assert_eq!(column.table, None);
        assert_eq!(column.column_type, ColumnType::Regular("id".to_string()));
    }

    #[test]
    fn test_parse_invalid_whitespace() {
        // Invalid whitespace in identifier
        assert!(all_consuming(Column::parse)("user name").is_err());
        // Invalid whitespace in function name
        assert!(all_consuming(Column::parse)("count name(id)").is_err());
        // Invalid whitespace in table name
        assert!(all_consuming(Column::parse)("user table.id").is_err());
    }
}
