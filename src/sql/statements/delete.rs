use crate::sql::clauses::where_clause::WhereClause;
use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace1},
    combinator::opt,
    IResult,
};

use super::Statement;
#[derive(Debug, PartialEq)]
pub enum DeleteStatement {
    FromTable(String, Option<WhereClause>),
}

impl DeleteStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("DELETE FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;

        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(WhereClause::parse)(input)?;
        Ok((
            input,
            Statement::Delete(DeleteStatement::FromTable(
                table_name.to_string(),
                where_clause,
            )),
        ))
    }
}
