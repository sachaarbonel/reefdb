use nom::{branch::alt, bytes::complete::tag_no_case, combinator::map, IResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    Text,
    Integer,
    TSVector,
    Boolean,
    Float,
    Date,
    Timestamp,
    Null
}

impl DataType {
    pub fn parse(input: &str) -> IResult<&str, DataType> {
        alt((
            map(tag_no_case("TEXT"), |_| DataType::Text),
            map(tag_no_case("INTEGER"), |_| DataType::Integer),
            map(alt((tag_no_case("TSVECTOR"), tag_no_case("TSVector"))), |_| DataType::TSVector),
            map(tag_no_case("BOOLEAN"), |_| DataType::Boolean),
            map(tag_no_case("FLOAT"), |_| DataType::Float),
            map(tag_no_case("DATE"), |_| DataType::Date),
            map(tag_no_case("TIMESTAMP"), |_| DataType::Timestamp),
            map(tag_no_case("NULL"), |_| DataType::Null),
        ))(input)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_test() {
        use crate::sql::data_type::DataType;

        assert_eq!(DataType::parse("TEXT"), Ok(("", DataType::Text)));
        assert_eq!(DataType::parse("INTEGER"), Ok(("", DataType::Integer)));
        assert_eq!(DataType::parse("TSVECTOR"), Ok(("", DataType::TSVector)));
        assert_eq!(DataType::parse("BOOLEAN"), Ok(("", DataType::Boolean)));
        assert_eq!(DataType::parse("FLOAT"), Ok(("", DataType::Float)));
        assert_eq!(DataType::parse("DATE"), Ok(("", DataType::Date)));
        assert_eq!(DataType::parse("TIMESTAMP"), Ok(("", DataType::Timestamp)));
        assert_eq!(DataType::parse("NULL"), Ok(("", DataType::Null)));
    }
}
