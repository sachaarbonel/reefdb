use nom::{
    bytes::complete::tag_no_case,
    character::complete::{multispace1, alphanumeric1},
    IResult,
};

use super::Statement;

#[derive(Debug, PartialEq)]
pub struct DropIndexStatement {
    pub table_name: String,
    pub column_name: String,
}

impl DropIndexStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("DROP INDEX ON")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("(")(input)?;
        let (input, column_name) = alphanumeric1(input)?;
        let (input, _) = tag_no_case(")")(input)?;
        
        Ok((
            input,
            Statement::DropIndex(DropIndexStatement {
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
            }),
        ))
    }
} 