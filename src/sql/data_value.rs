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
            (DataValue::Text(_), DataType::FTSText) => true,
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
    let (input, value) = delimited(
        tag("'"),
        recognize(nom::multi::many1(nom::character::complete::none_of("'"))),
        tag("'"),
    )(input)?;

    Ok((input, DataValue::Text(value.to_string())))
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
    }

    #[test]
    fn comparison_test() {
        assert!(DataValue::Integer(5) > DataValue::Integer(3));
        assert!(DataValue::Integer(3) < DataValue::Integer(5));
        assert!(DataValue::Integer(3) <= DataValue::Integer(3));
        assert!(DataValue::Integer(3) >= DataValue::Integer(3));
        assert!(DataValue::Text("b".to_string()) > DataValue::Text("a".to_string()));
        assert!(DataValue::Text("a".to_string()) < DataValue::Text("b".to_string()));
    }
}
