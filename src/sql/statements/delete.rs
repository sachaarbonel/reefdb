use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace1},
    combinator::opt,
    IResult,
};

use crate::sql::clauses::wheres::where_type::WhereType;

use super::Statement;
#[derive(Debug, PartialEq, Clone)]
pub enum DeleteStatement {
    FromTable(String, Option<WhereType>),
}

impl DeleteStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("DELETE FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;

        let (input, _) = opt(multispace1)(input)?;
        let (input, where_clause) = opt(WhereType::parse)(input)?;
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
    use crate::sql::{
        clauses::wheres::{where_clause::WhereClause, where_type::WhereType},
        statements::Statement,
    };

    #[test]
    fn parse_test() {
        use super::DeleteStatement;
        use crate::sql::data_value::DataValue;

        assert_eq!(
            DeleteStatement::parse("DELETE FROM users WHERE id = 1"),
            Ok((
                "",
                Statement::Delete(DeleteStatement::FromTable(
                    "users".to_string(),
                    Some(WhereType::Regular(WhereClause {
                        col_name: "id".to_string(),
                        // operator: Operator::Equal,
                        value: DataValue::Integer(1),
                    }))
                ))
            ))
        );
    }
}
