use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace1},
    IResult,
};

use super::data_value::DataValue;

#[derive(Debug)]
pub struct WhereClause {
   pub col_name: String,
   pub value: DataValue,
}

impl WhereClause {
    pub fn parse(input: &str) -> IResult<&str, WhereClause> {
        let (input, _) = tag("WHERE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, col_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("=")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, value) = DataValue::parse(input)?;

        Ok((
            input,
            WhereClause {
                col_name: col_name.to_string(),
                value: value,
            },
        ))
    }
}
