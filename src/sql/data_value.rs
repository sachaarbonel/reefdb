use nom::IResult;
use nom::{branch::alt, bytes::complete::tag, combinator::recognize, sequence::delimited};
use serde::{Deserialize, Serialize};
use crate::sql::data_type::DataType;
use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum DataValue {
    Text(String),
    Integer(i32),
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (DataValue::Integer(a), DataValue::Integer(b)) => a.cmp(b),
            (DataValue::Text(a), DataValue::Text(b)) => a.cmp(b),
            (DataValue::Integer(_), DataValue::Text(_)) => Ordering::Less,
            (DataValue::Text(_), DataValue::Integer(_)) => Ordering::Greater,
        }
    }
}

impl DataValue {
    pub fn parse(input: &str) -> IResult<&str, DataValue> {
        alt((parse_quoted_text, parse_integer))(input)
    }

    pub fn matches_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (DataValue::Text(_), DataType::Text) => true,
            (DataValue::Text(_), DataType::TSVector) => true,
            (DataValue::Integer(_), DataType::Integer) => true,
            _ => false,
        }
    }
}

fn parse_integer(input: &str) -> IResult<&str, DataValue> {
    let (input, value) = nom::character::complete::digit1(input)?;
    Ok((input, DataValue::Integer(value.parse().unwrap())))
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
                } else {
                    // This is the end of the string
                    return Ok((&input[pos..], DataValue::Text(result)));
                }
            } else {
                // End of input after quote
                return Ok((&input[pos..], DataValue::Text(result)));
            }
        } else {
            result.push(c);
        }
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Tag,
    )))
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
            DataValue::parse("'Hello''World'"),
            Ok(("", DataValue::Text("Hello'World".to_string())))
        );
    }

    #[test]
    fn comparison_test() {
        let a = DataValue::Integer(1);
        let b = DataValue::Integer(2);
        let c = DataValue::Text("hello".to_string());
        let d = DataValue::Text("world".to_string());

        assert!(a < b);
        assert!(c < d);
        assert!(a < c);
        assert!(b < d);
    }
}
