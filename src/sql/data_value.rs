use crate::IResult;
use nom::{branch::alt, bytes::complete::tag, combinator::recognize, sequence::delimited};
use serde::{Deserialize, Serialize};
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataValue {
    Text(String),
    Integer(i32),
}

impl DataValue {
    pub fn parse(input: &str) -> IResult<&str, DataValue> {
        alt((parse_quoted_text, parse_integer))(input)
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
    #[test]
    fn parse_test() {
     
        use crate::sql::data_value::DataValue;
        assert_eq!(
            DataValue::parse("'Hello World'"),
            Ok(("", DataValue::Text("Hello World".to_string())))
        );
        assert_eq!(
            DataValue::parse("123"),
            Ok(("", DataValue::Integer(123)))
        );
    }
}
