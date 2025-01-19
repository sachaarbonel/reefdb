// function_parser.rs
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    combinator::{map, opt},
    multi::separated_list0,
    sequence::{delimited, tuple},
    IResult,
    error::Error,
};
use crate::sql::data_value::DataValue;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<DataValue>,
    pub alias: Option<String>,
}

// Identifier characters for arguments (allowing dots for table.column)
fn is_argument_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '.'
}

// Identifier for function names (no dots)
fn is_function_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

// Parser for identifiers (used for aliases)
fn identifier_no_space(input: &str) -> IResult<&str, &str> {
    nom::bytes::complete::take_while1(is_argument_identifier_char)(input)
}

// Parser for function names (no dots)
fn function_identifier(input: &str) -> IResult<&str, &str> {
    nom::bytes::complete::take_while1(is_function_identifier_char)(input)
}

// Parser for arguments (allows dots)
fn identifier(input: &str) -> IResult<&str, &str> {
    delimited(
        multispace0,
        identifier_no_space,
        multispace0
    )(input)
}

// Parser for a single argument
fn parse_argument(input: &str) -> IResult<&str, DataValue> {
    alt((
        map(tag("*"), |s: &str| DataValue::Text(s.to_string())),
        map(
            delimited(
                tag("'"),
                nom::bytes::complete::take_until("'"),
                tag("'")
            ),
            |s: &str| DataValue::Text(s.to_string())
        ),
        map(parse_function_call, |f| DataValue::Function {
            name: f.name,
            args: f.args,
        }),
        map(identifier, |s: &str| DataValue::Text(s.to_string())),
    ))(input)
}

// Parser for nested function calls (without alias)
fn parse_function_call(input: &str) -> IResult<&str, FunctionCall> {
    let (input, name) = function_identifier(input)?;
    let (input, _) = tag("(")(input)?;
    let (input, args) = separated_list0(
        tuple((multispace0, tag(","), multispace0)),
        parse_argument
    )(input)?;
    let (input, _) = tag(")")(input)?;
    Ok((input, FunctionCall {
        name: name.to_string(),
        args,
        alias: None, // Nested functions do not have aliases
    }))
}

// Top-level function parser (handles aliases)
pub fn parse_function(input: &str) -> IResult<&str, FunctionCall> {
    let (input, name) = function_identifier(input)?;
    let (input, _) = tag("(")(input)?;
    let (input, args) = separated_list0(
        tuple((multispace0, tag(","), multispace0)),
        parse_argument
    )(input)?;
    let (input, _) = tag(")")(input)?;
    let (input, alias) = opt(tuple((
        multispace1,
        tag_no_case("as"),
        multispace1,
        identifier_no_space
    )))(input)?;
    let alias = alias.map(|(_, _, _, alias)| alias.to_string());
    Ok((input, FunctionCall {
        name: name.to_string(),
        args,
        alias,
    }))
}

impl fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        write!(f, ")")?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_function() {
        let input = "count(*)";
        let (remaining, result) = parse_function(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(result.name, "count");
        assert_eq!(result.args, vec![DataValue::Text("*".to_string())]);
        assert_eq!(result.alias, None);
    }

    #[test]
    fn test_parse_function_with_alias() {
        let input = "count(*) as total";
        let (remaining, result) = parse_function(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(result.name, "count");
        assert_eq!(result.args, vec![DataValue::Text("*".to_string())]);
        assert_eq!(result.alias, Some("total".to_string()));
    }

    #[test]
    fn test_parse_function_with_string_args() {
        let input = "to_tsquery('rust & web')";
        let (remaining, result) = parse_function(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(result.name, "to_tsquery");
        assert_eq!(result.args, vec![DataValue::Text("rust & web".to_string())]);
    }

    #[test]
    fn test_parse_nested_function() {
        let input = "ts_rank(to_tsvector(content), to_tsquery('rust')) as rank";
        let (remaining, result) = parse_function(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(result.name, "ts_rank");
        assert_eq!(result.args.len(), 2);
        assert_eq!(result.alias, Some("rank".to_string()));
        
        // Check nested functions
        match &result.args[0] {
            DataValue::Function { name, args } => {
                assert_eq!(name, "to_tsvector");
                assert_eq!(*args, vec![DataValue::Text("content".to_string())]);
            },
            _ => panic!("Expected a nested function"),
        }

        match &result.args[1] {
            DataValue::Function { name, args } => {
                assert_eq!(name, "to_tsquery");
                assert_eq!(*args, vec![DataValue::Text("rust".to_string())]);
            },
            _ => panic!("Expected a nested function"),
        }
    }
}
