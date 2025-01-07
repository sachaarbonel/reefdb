use nom::{
    bytes::complete::tag_no_case,
    character::complete::{multispace1, alphanumeric1},
    IResult,
};

use super::Statement;

#[derive(Debug, PartialEq)]
pub struct DropStatement {
    pub table_name: String,
}

impl DropStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("DROP TABLE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        
        Ok((
            input,
            Statement::Drop(DropStatement {
                table_name: table_name.to_string(),
            }),
        ))
    }
} 