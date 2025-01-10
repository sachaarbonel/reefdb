use nom::{
    bytes::complete::tag_no_case,
    character::complete::multispace1,
    combinator::opt,
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
pub enum DeleteStatement {
    FromTable(String, Option<WhereType>),
}

impl DeleteStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("DELETE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = table_name(input)?;
        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(parse_where_clause)(input)?;

        Ok((
            input,
            Statement::Delete(DeleteStatement::FromTable(
                table_name.to_string(),
                where_clause,
            )),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::statements::Statement;

    #[test]
    fn parse_delete_test() {
        assert_eq!(
            DeleteStatement::parse("DELETE FROM users"),
            Ok((
                "",
                Statement::Delete(DeleteStatement::FromTable(
                    "users".to_string(),
                    None
                ))
            ))
        );
    }

    #[test]
    fn parse_delete_with_where_test() {
        let res = DeleteStatement::parse("DELETE FROM users WHERE id = 1");
        assert_eq!(
            res,
            Ok((
                "",
                Statement::Delete(DeleteStatement::FromTable(
                    "users".to_string(),
                    Some(WhereType::Regular(WhereClause::new(
                        "id".to_string(),
                        Op::Equal,
                        DataValue::Integer(1),
                        None,
                    )))
                ))
            ))
        );
    }

    #[test]
    fn parse_delete_with_where_text_test() {
        let res = DeleteStatement::parse("DELETE FROM users WHERE status = 'inactive'");
        assert_eq!(
            res,
            Ok((
                "",
                Statement::Delete(DeleteStatement::FromTable(
                    "users".to_string(),
                    Some(WhereType::Regular(WhereClause::new(
                        "status".to_string(),
                        Op::Equal,
                        DataValue::Text("inactive".to_string()),
                        None,
                    )))
                ))
            ))
        );
    }
}
