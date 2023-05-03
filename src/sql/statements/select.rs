use crate::sql::where_clause::WhereClause;
use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, multispace1},
    combinator::{opt, recognize, map},
    multi::separated_list0,
    sequence::terminated,
    IResult,
};

use super::Statement;

#[derive(Debug)]
pub enum SelectStatement {
    FromTable(String, Vec<String>, Option<WhereClause>),
}

impl SelectStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("SELECT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, columns) = parse_column_list(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;

        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(WhereClause::parse)(input)?;
        Ok((
            input,
            Statement::Select(SelectStatement::FromTable(
                table_name.to_string(),
                columns,
                where_clause,
            )),
        ))
    }
}

fn parse_column_list(input: &str) -> IResult<&str, Vec<String>> {
    separated_list0(
        terminated(tag(","), multispace0),
        map(recognize(alphanumeric1), String::from),
    )(input)
}
