use nom::IResult;
use nom::{branch::alt, bytes::complete::{tag, tag_no_case}, combinator::{recognize, map}, sequence::delimited};
use serde::{Deserialize, Serialize};
use crate::sql::data_type::DataType;
use std::cmp::Ordering;
use chrono::{NaiveDate, NaiveDateTime};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataValue {
    Text(String),
    Integer(i32),
    Boolean(bool),
    Float(f64),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Null,
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (DataValue::Integer(a), DataValue::Integer(b)) => Some(a.cmp(b)),
            (DataValue::Text(a), DataValue::Text(b)) => Some(a.cmp(b)),
            (DataValue::Boolean(a), DataValue::Boolean(b)) => Some(a.cmp(b)),
            (DataValue::Float(a), DataValue::Float(b)) => a.partial_cmp(b),
            (DataValue::Date(a), DataValue::Date(b)) => Some(a.cmp(b)),
            (DataValue::Timestamp(a), DataValue::Timestamp(b)) => Some(a.cmp(b)),
            (DataValue::Null, DataValue::Null) => Some(Ordering::Equal),
            (DataValue::Null, _) => Some(Ordering::Less),
            (_, DataValue::Null) => Some(Ordering::Greater),
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
    pub fn parse(input: &str) -> IResult<&str, DataValue> {
        alt((
            parse_date,
            parse_timestamp,
            parse_quoted_text,
            parse_integer,
            parse_float,
            parse_boolean,
            parse_null,
        ))(input)
    }

    pub fn matches_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (DataValue::Text(_), DataType::Text) => true,
            (DataValue::Text(_), DataType::TSVector) => true,
            (DataValue::Integer(_), DataType::Integer) => true,
            (DataValue::Boolean(_), DataType::Boolean) => true,
            (DataValue::Float(_), DataType::Float) => true,
            (DataValue::Date(_), DataType::Date) => true,
            (DataValue::Timestamp(_), DataType::Timestamp) => true,
            (DataValue::Null, _) => true,
            _ => false,
        }
    }
}

fn parse_integer(input: &str) -> IResult<&str, DataValue> {
    let (input, value) = nom::character::complete::digit1(input)?;
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
    let (input, value) = nom::number::complete::double(input)?;
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
    
    match NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        Ok(date) => Ok((input, DataValue::Date(date))),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        ))),
    }
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
    
    match NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
        Ok(timestamp) => Ok((input, DataValue::Timestamp(timestamp))),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        ))),
    }
}

fn parse_null(input: &str) -> IResult<&str, DataValue> {
    map(tag("NULL"), |_| DataValue::Null)(input)
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

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
            Ok(("", DataValue::Date(NaiveDate::from_ymd_opt(2024, 3, 14).unwrap())))
        );
        assert_eq!(
            DataValue::parse("'2024-03-14 12:34:56'"),
            Ok(("", DataValue::Timestamp(
                NaiveDateTime::parse_from_str("2024-03-14 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap()
            )))
        );
        assert_eq!(
            DataValue::parse("NULL"),
            Ok(("", DataValue::Null))
        );
    }
}
