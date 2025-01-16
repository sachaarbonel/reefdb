use nom::{
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1, alphanumeric1},
    combinator::{map, opt},
    sequence::{delimited, tuple},
    IResult,
};

use crate::sql::{
    clauses::wheres::where_type::{parse_where_clause, WhereType},
    statements::Statement,
};

#[derive(Debug, PartialEq, Clone)]
pub enum DeleteStatement {
    FromTable(String, Option<WhereType>),
}

impl DeleteStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = delimited(
            multispace0,
            tag_no_case("DELETE"),
            multispace1
        )(input)?;

        let (input, _) = delimited(
            multispace0,
            tag_no_case("FROM"),
            multispace1
        )(input)?;

        let (input, table_name) = delimited(
            multispace0,
            alphanumeric1,
            multispace0
        )(input)?;

        let (input, where_clause) = opt(parse_where_clause)(input)?;

        Ok((input, Statement::Delete(DeleteStatement::FromTable(
            table_name.to_string(),
            where_clause,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{
        clauses::wheres::where_clause::WhereClause,
        data_value::DataValue,
        operators::op::Op,
    };

    #[test]
    fn parse_delete_test() {
        let input = "DELETE FROM users";
        let (remaining, stmt) = DeleteStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Delete(DeleteStatement::FromTable(table_name, where_clause)) => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn parse_delete_with_where_test() {
        let input = "DELETE FROM users WHERE id = 1";
        let (remaining, stmt) = DeleteStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Delete(DeleteStatement::FromTable(table_name, Some(WhereType::Regular(where_clause)))) => {
                assert_eq!(table_name, "users");
                assert_eq!(where_clause.col_name, "id");
                assert_eq!(where_clause.operator, Op::Equal);
                assert_eq!(where_clause.value, DataValue::Integer(1));
            }
            _ => panic!("Expected Delete statement with where clause"),
        }
    }

    #[test]
    fn parse_delete_with_where_text_test() {
        let input = "DELETE FROM users WHERE status = 'inactive'";
        let (remaining, stmt) = DeleteStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Delete(DeleteStatement::FromTable(table_name, Some(WhereType::Regular(where_clause)))) => {
                assert_eq!(table_name, "users");
                assert_eq!(where_clause.col_name, "status");
                assert_eq!(where_clause.operator, Op::Equal);
                assert_eq!(where_clause.value, DataValue::Text("inactive".to_string()));
            }
            _ => panic!("Expected Delete statement with where clause"),
        }
    }
}
