use nom::{branch::alt, bytes::complete::tag, combinator::map, IResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Text,
    Integer,
}

impl DataType {
    pub fn parse(input: &str) -> IResult<&str, DataType> {
        alt((
            map(tag("TEXT"), |_| DataType::Text),
            map(tag("INTEGER"), |_| DataType::Integer),
        ))(input)
    }
}
