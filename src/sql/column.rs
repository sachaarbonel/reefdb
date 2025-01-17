use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
    combinator::{map, opt, recognize, verify},
    multi::{many0, separated_list0},
    sequence::{delimited, preceded, tuple},
};

use crate::sql::data_value::DataValue;

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
            Self::parse_function,
            Self::parse_wildcard,
            Self::parse_table_column,
            Self::parse_regular_column,
        ))(input)
    }

    fn parse_function(input: &str) -> IResult<&str, Self> {
        let (input, name) = identifier(input)?;
        let (input, _) = preceded(multispace0, tag("("))(input)?;
        let (input, args) = delimited(
            multispace0,
            separated_list0(
                delimited(multispace0, tag(","), multispace0),
                alt((
                    map(Self::parse, |col| match col.column_type {
                        ColumnType::Regular(name) => DataValue::Text(name),
                        ColumnType::Function(name, args) => DataValue::Function {
                            name,
                            args,
                        },
                        ColumnType::Wildcard => DataValue::Text("*".to_string()),
                    }),
                    map(identifier, |s| DataValue::Text(s.to_string())),
                    map(
                        delimited(
                            tag("'"),
                            recognize(many0(alt((
                                alphanumeric1,
                                tag(" "),
                                tag("&"),
                                tag("|"),
                                tag("!"),
                                tag("_"),
                                tag("-"),
                                tag("."),
                                tag("("),
                                tag(")"),
                            )))),
                            tag("'")
                        ),
                        |s: &str| DataValue::Text(s.to_string())
                    ),
                ))
            ),
            multispace0
        )(input)?;
        let (input, _) = preceded(multispace0, tag(")"))(input)?;
        let (input, alias) = opt(preceded(
            delimited(multispace0, tag_no_case("as"), multispace1),
            identifier
        ))(input)?;
        let (input, _) = multispace0(input)?;

        Ok((input, Column {
            table: None,
            name: alias.map(|s| s.to_string()).unwrap_or_else(|| name.to_string()),
            column_type: ColumnType::Function(name.to_string(), args),
        }))
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
        assert_eq!(column.name, "count");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(_, _)));
    }

    #[test]
    fn test_parse_function_with_table_column() {
        let input = "concat(users.first_name, users.last_name)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "concat");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(_, _)));
    }

    #[test]
    fn test_parse_function_with_multiple_args() {
        let input = "concat(first_name, last_name)";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "concat");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(_, _)));
    }

    #[test]
    fn test_parse_with_whitespace() {
        let input = "  name  ";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "name");
        assert_eq!(column.table, None);
        assert_eq!(column.column_type, ColumnType::Regular("name".to_string()));
    }

   
    #[test]
    fn test_parse_invalid_input() {
        let input = "123name";
        assert!(Column::parse(input).is_err());
    }

    #[test]
    fn test_parse_function_with_alias() {
        let input = "count(*) as total";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "total");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(name, _) if name == "count"));
    }

    #[test]
    fn test_parse_function_with_complex_alias() {
        let input = "ts_rank(to_tsvector(content),to_tsquery('rust')) as rank";
        let (remaining, column) = Column::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(column.name, "rank");
        assert_eq!(column.table, None);
        assert!(matches!(column.column_type, ColumnType::Function(name, _) if name == "ts_rank"));
    }
}
