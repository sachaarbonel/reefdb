use nom::{branch::alt, bytes::complete::tag, combinator::map, IResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    Text,
    Integer,
    FTSText
}

impl DataType {
    pub fn parse(input: &str) -> IResult<&str, DataType> {
        alt((
            map(tag("TEXT"), |_| DataType::Text),
            map(tag("INTEGER"), |_| DataType::Integer),
            map(tag("FTS_TEXT"), |_| DataType::FTSText),
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
        assert_eq!(DataType::parse("FTS_TEXT"), Ok(("", DataType::FTSText)));
    }
}
