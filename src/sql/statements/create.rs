use crate::sql::column_def::ColumnDef;
use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, multispace1},
    multi::separated_list0,
    sequence::{delimited, terminated},
    IResult,
};

use super::Statement;

#[derive(Debug)]
pub enum CreateStatement {
    Table(String, Vec<ColumnDef>),
}

impl CreateStatement {
   pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("CREATE TABLE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, columns) = delimited(
            tag("("),
            separated_list0(terminated(tag(","), multispace0), ColumnDef::parse),
            tag(")"),
        )(input)?;

        Ok((
            input,
            Statement::Create(CreateStatement::Table(table_name.to_string(), columns)),
        ))
    }
}
