use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, digit1},
    combinator::{map, opt, value, recognize},
    multi::separated_list0,
    number::complete::double,
    sequence::{delimited, preceded, tuple},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::{fmt, cmp::Ordering};
use crate::fts::text_processor::{TsVector, TSQuery};

use crate::sql::{
    data_type::DataType,
    parser_utils::{ident, ident_allow_dot},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataValue {
    Text(String),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    Date(String),
    Timestamp(String),
    TSVector(TsVector),
    TSQuery(TSQuery),
    Null,
    Function {
        name: String,
        args: Vec<DataValue>,
    },
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (DataValue::Integer(a), DataValue::Integer(b)) => Some(a.cmp(b)),
            (DataValue::Text(a), DataValue::Text(b)) => Some(a.cmp(b)),
            (DataValue::TSVector(a), DataValue::TSVector(b)) => Some(a.tokens.len().cmp(&b.tokens.len())),
            (DataValue::TSQuery(a), DataValue::TSQuery(b)) => Some(a.tokens.len().cmp(&b.tokens.len())),
            (DataValue::Boolean(a), DataValue::Boolean(b)) => Some(a.cmp(b)),
            (DataValue::Float(a), DataValue::Float(b)) => a.partial_cmp(b),
            (DataValue::Date(a), DataValue::Date(b)) => Some(a.cmp(b)),
            (DataValue::Timestamp(a), DataValue::Timestamp(b)) => Some(a.cmp(b)),
            (DataValue::Null, DataValue::Null) => Some(Ordering::Equal),
            (DataValue::Null, _) => Some(Ordering::Less),
            (_, DataValue::Null) => Some(Ordering::Greater),
            (DataValue::Function { .. }, _) => None,
            (_, DataValue::Function { .. }) => None,
            _ => None,
        }
    }
}

impl Eq for DataValue {}

impl Ord for DataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Less)
    }
}

impl DataValue {
    pub fn matches_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (DataValue::Text(_), DataType::Text) => true,
            (DataValue::TSVector(_), DataType::TSVector) => true,
            (DataValue::Text(_), DataType::TSVector) => true,
            (DataValue::Integer(_), DataType::Integer) => true,
            (DataValue::Boolean(_), DataType::Boolean) => true,
            (DataValue::Float(_), DataType::Float) => true,
            (DataValue::Date(_), DataType::Date) => true,
            (DataValue::Timestamp(_), DataType::Timestamp) => true,
            (DataValue::Null, _) => true,
            (DataValue::Function { .. }, _) => true,
            _ => false,
        }
    }

    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (input, _) = multispace0(input)?;
        alt((
            Self::parse_function,
            Self::parse_date,
            Self::parse_timestamp,
            Self::parse_quoted_text,
            Self::parse_integer,
            Self::parse_float,
            Self::parse_boolean,
            Self::parse_null,
        ))(input)
    }

    fn parse_integer(input: &str) -> IResult<&str, DataValue> {
        let (input, value) = recognize(tuple((
            opt(tag("-")),
            digit1
        )))(input)?;
        
        // Check if the next character is a decimal point
        if input.starts_with('.') {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Digit,
            )));
        }
        
        Ok((input, DataValue::Integer(value.parse().unwrap())))
    }
    
    fn parse_boolean(input: &str) -> IResult<&str, DataValue> {
        alt((
            map(tag_no_case("TRUE"), |_| DataValue::Boolean(true)),
            map(tag_no_case("FALSE"), |_| DataValue::Boolean(false)),
        ))(input)
    }
    
    fn parse_float(input: &str) -> IResult<&str, DataValue> {
        let (input, value) = double(input)?;
        Ok((input, DataValue::Float(value)))
    }
    
    fn parse_date(input: &str) -> IResult<&str, DataValue> {
        let (input, date_str) = delimited(
            tag("'"),
            recognize(nom::sequence::tuple((
                nom::character::complete::digit1,
                tag("-"),
                nom::character::complete::digit1,
                tag("-"),
                nom::character::complete::digit1,
            ))),
            tag("'"),
        )(input)?;
        
        Ok((input, DataValue::Date(date_str.to_string())))
    }
    
    fn parse_timestamp(input: &str) -> IResult<&str, DataValue> {
        let (input, timestamp_str) = delimited(
            tag("'"),
            recognize(nom::sequence::tuple((
                nom::character::complete::digit1,
                tag("-"),
                nom::character::complete::digit1,
                tag("-"),
                nom::character::complete::digit1,
                tag(" "),
                nom::character::complete::digit1,
                tag(":"),
                nom::character::complete::digit1,
                tag(":"),
                nom::character::complete::digit1,
            ))),
            tag("'"),
        )(input)?;
        
        Ok((input, DataValue::Timestamp(timestamp_str.to_string())))
    }
    
    fn parse_null(input: &str) -> IResult<&str, DataValue> {
        map(tag_no_case("NULL"), |_| DataValue::Null)(input)
    }
    
    fn parse_quoted_text(input: &str) -> IResult<&str, DataValue> {
        let (input, _) = tag("'")(input)?;
        let mut result = String::new();
        let mut chars = input.chars();
        let mut pos = 0;
    
        while let Some(c) = chars.next() {
            pos += 1;
            if c == '\'' {
                // Look ahead for another quote
                if let Some(next_c) = chars.clone().next() {
                    if next_c == '\'' {
                        // This is an escaped quote
                        result.push('\'');
                        chars.next(); // Skip the next quote
                        pos += 1;
                        continue;
                    }
                }
                // This is the end of the string
                return Ok((&input[pos..], DataValue::Text(result)));
            } else {
                result.push(c);
            }
        }
    
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )))
    }

    pub fn parse_function(input: &str) -> IResult<&str, DataValue> {
        let (input, (name, _, args)) = tuple((
            preceded(multispace0, ident),
            preceded(multispace0, tag("(")),
            delimited(
                multispace0,
                separated_list0(
                    delimited(multispace0, tag(","), multispace0),
                    alt((
                        Self::parse_function,
                        Self::parse_quoted_text,
                        Self::parse_array,
                        Self::parse_integer,
                        Self::parse_float,
                        Self::parse_boolean,
                        Self::parse_null,
                        map(ident_allow_dot, |s: &str| DataValue::Text(s.to_string())),
                    )),
                ),
                tuple((multispace0, tag(")"))),
            ),
        ))(input)?;

        Ok((
            input,
            DataValue::Function {
                name: name.to_string(),
                args,
            },
        ))
    }

    pub fn parse_array(input: &str) -> IResult<&str, DataValue> {
        let (input, elements) = delimited(
            tuple((multispace0, tag("["), multispace0)),
            separated_list0(
                delimited(multispace0, tag(","), multispace0),
                alt((
                    Self::parse_float,
                    map(ident, |s: &str| DataValue::Text(s.to_string())),
                )),
            ),
            tuple((multispace0, tag("]"), multispace0)),
        )(input)?;

        Ok((input, DataValue::Text(format!("[{}]", elements.iter().map(|e| match e {
            DataValue::Float(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f) // Always show one decimal place for whole numbers
                } else {
                    f.to_string()
                }
            },
            DataValue::Text(s) => s.clone(),
            _ => "".to_string(),
        }).collect::<Vec<_>>().join(", ")))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_test() {
        assert_eq!(
            DataValue::parse("'Hello World'"),
            Ok(("", DataValue::Text("Hello World".to_string())))
        );
        assert_eq!(
            DataValue::parse("123"),
            Ok(("", DataValue::Integer(123)))
        );
        assert_eq!(
            DataValue::parse("TRUE"),
            Ok(("", DataValue::Boolean(true)))
        );
        assert_eq!(
            DataValue::parse("FALSE"),
            Ok(("", DataValue::Boolean(false)))
        );
        assert_eq!(
            DataValue::parse("123.45"),
            Ok(("", DataValue::Float(123.45)))
        );
        assert_eq!(
            DataValue::parse("'2024-03-14'"),
            Ok(("", DataValue::Date("2024-03-14".to_string())))
        );
        assert_eq!(
            DataValue::parse("'2024-03-14 12:34:56'"),
            Ok(("", DataValue::Timestamp("2024-03-14 12:34:56".to_string())))
        );
        assert_eq!(
            DataValue::parse("NULL"),
            Ok(("", DataValue::Null))
        );

        // Test function parsing
        assert_eq!(
            DataValue::parse("ts_rank(vector, query)"),
            Ok(("", DataValue::Function {
                name: "ts_rank".to_string(),
                args: vec![
                    DataValue::Text("vector".to_string()),
                    DataValue::Text("query".to_string()),
                ],
            }))
        );

        assert_eq!(
            DataValue::parse("ts_rank(vector, query, 1)"),
            Ok(("", DataValue::Function {
                name: "ts_rank".to_string(),
                args: vec![
                    DataValue::Text("vector".to_string()),
                    DataValue::Text("query".to_string()),
                    DataValue::Integer(1),
                ],
            }))
        );

        assert_eq!(
            DataValue::parse("ts_rank([0.1, 0.2, 0.4, 1.0], vector, query)"),
            Ok(("", DataValue::Function {
                name: "ts_rank".to_string(),
                args: vec![
                    DataValue::Text("[0.1, 0.2, 0.4, 1.0]".to_string()),
                    DataValue::Text("vector".to_string()),
                    DataValue::Text("query".to_string()),
                ],
            }))
        );
        
        assert_eq!(
            DataValue::parse("ts_rank(to_tsvector(content),to_tsquery('rust'))"),
            Ok(("", DataValue::Function {
                name: "ts_rank".to_string(),
                args: vec![
                    DataValue::Function {
                        name: "to_tsvector".to_string(),
                        args: vec![DataValue::Text("content".to_string())],
                    },
                    DataValue::Function {
                        name: "to_tsquery".to_string(),
                        args: vec![DataValue::Text("rust".to_string())],
                    },
                ],
            }))
        );
    }
}
