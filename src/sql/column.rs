// column.rs

use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::many0,
    sequence::{tuple, delimited},
};
use crate::sql::data_value::DataValue;
use super::function_parser::{parse_function, FunctionCall};

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub table: Option<String>,
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Regular(String),
    Wildcard,
    Function(String, Vec<DataValue>),
}

impl Column {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (input, _) = multispace0(input)?;
        alt((
            map(parse_function, |f: FunctionCall| Column {
                table: None,
                name: f.alias.unwrap_or_else(|| {
                    // For complex functions, use a formatted string as the name
                    format!("{}({})", f.name, f.args.iter()
                        .map(|arg| match arg {
                            DataValue::Function { name, args } => {
                                format!("{}({})", name, args.iter()
                                    .map(|a| a.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                                )
                            },
                            DataValue::Text(s) => s.to_string(),
                            _ => arg.to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", "))
                }),
                column_type: ColumnType::Function(f.name, f.args),
            }),
            Self::parse_wildcard,
            Self::parse_table_column,
            Self::parse_regular_column,
        ))(input)
    }

    fn parse_wildcard(input: &str) -> IResult<&str, Self> {
        let (input, _) = tag("*")(input)?;
        Ok((input, Column {
            table: None,
            name: "*".to_string(),
            column_type: ColumnType::Wildcard,
        }))
    }

    pub fn parse_table_column(input: &str) -> IResult<&str, Self> {
        let (input, _) = multispace0(input)?;
        let (input, table) = opt(tuple((
            identifier_no_space,
            tag(".")
        )))(input)?;
        let (input, name) = identifier_no_space(input)?;
        let (input, _) = multispace0(input)?;

        Ok((input, Column {
            table: table.map(|(t, _)| t.to_string()),
            name: name.to_string(),
            column_type: ColumnType::Regular(name.to_string()),
        }))
    }

    fn parse_regular_column(input: &str) -> IResult<&str, Self> {
        let (input, name) = identifier(input)?;
        Ok((input, Column {
            table: None,
            name: name.to_string(),
            column_type: ColumnType::Regular(name.to_string()),
        }))
    }
}

fn identifier(input: &str) -> IResult<&str, &str> {
    delimited(
        multispace0,
        identifier_no_space,
        multispace0
    )(input)
}

fn identifier_no_space(input: &str) -> IResult<&str, &str> {
    recognize(
        tuple((
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        ))
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_regular_column() {
        let input = "name";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "name");
        assert_eq!(column.table, None);
        assert_eq!(column.column_type, ColumnType::Regular("name".to_string()));
    }

    #[test]
    fn test_parse_table_column() {
        let input = "users.name";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "name");
        assert_eq!(column.table, Some("users".to_string()));
        assert_eq!(column.column_type, ColumnType::Regular("name".to_string()));
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
        assert_eq!(column.name, "count(*)");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(_, _)));
    }

    #[test]
    fn test_parse_function_with_table_column() {
        let input = "concat(users.first_name, users.last_name)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "concat(users.first_name, users.last_name)");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(_, _)));
    }

    #[test]
    fn test_parse_function_with_multiple_args() {
        let input = "concat(first_name, last_name)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "concat(first_name, last_name)");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(_, _)));
    }

    #[test]
    fn test_parse_nested_function() {
        let input = "ts_rank(to_tsvector(content), to_tsquery('rust')) as rank";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "rank");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(name, _) if name == "ts_rank"));
    }
}
