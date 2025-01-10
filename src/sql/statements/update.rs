use nom::{
    bytes::complete::{tag, tag_no_case},
    character::complete::{alphanumeric1, multispace1},
    combinator::opt,
    multi::separated_list0,
    sequence::terminated,
    IResult,
};

use crate::sql::{
    clauses::wheres::where_type::{WhereType, WhereClause, parse_where_clause},
    data_value::DataValue,
    operators::op::Op,
};
use crate::sql::column_def::table_name;
use super::Statement;

#[derive(Debug, PartialEq, Clone)]
pub enum UpdateStatement {
    UpdateTable(String, Vec<(String, DataValue)>, Option<WhereType>),
}

impl UpdateStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("UPDATE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = table_name(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("SET")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, updates) = separated_list0(
            terminated(tag(","), multispace1),
            parse_column_value_pair,
        )(input)?;
        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(parse_where_clause)(input)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::statements::Statement;

    #[test]
    fn parse_test() {
        assert_eq!(
            UpdateStatement::parse("UPDATE users SET id = 1, name = 'John' WHERE id = 1"),
            Ok((
                "",
                Statement::Update(UpdateStatement::UpdateTable(
                    "users".to_string(),
                    vec![
                        ("id".to_string(), DataValue::Integer(1)),
                        ("name".to_string(), DataValue::Text("John".to_string())),
                    ],
                    Some(WhereType::Regular(WhereClause::new(
                        "id".to_string(),
                        Op::Equal,
                        DataValue::Integer(1),
                        None
                    )))
                ))
            ))
        );
    }
}
