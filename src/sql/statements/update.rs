use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, multispace1},
    combinator::opt,
    multi::separated_list0,
    sequence::terminated,
    IResult,
};

use crate::sql::{data_value::DataValue, where_clause::WhereClause};

use super::Statement;

#[derive(Debug)]
pub enum UpdateStatement {
    UpdateTable(String, Vec<(String, DataValue)>, Option<WhereClause>),
}

impl UpdateStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("UPDATE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("SET")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, updates) =
            separated_list0(terminated(tag(","), multispace0), parse_column_value_pair)(input)?;
        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(WhereClause::parse)(input)?;

        Ok((
            input,
            Statement::Update(UpdateStatement::UpdateTable(
                table_name.to_string(),
                updates,
                where_clause,
            )),
        ))
    }
}

fn parse_column_value_pair(input: &str) -> IResult<&str, (String, DataValue)> {
    let (input, col_name) = alphanumeric1(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, value) = DataValue::parse(input)?;

    Ok((input, (col_name.to_string(), value)))
}
