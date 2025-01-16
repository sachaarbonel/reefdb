use nom::{
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1, alphanumeric1},
    combinator::{map, opt},
    sequence::{delimited, tuple},
    multi::separated_list1,
    IResult,
};

use crate::sql::{
    clauses::wheres::where_type::{parse_where_clause, WhereType},
    data_value::DataValue,
    statements::Statement,
};

#[derive(Debug, PartialEq, Clone)]
pub enum UpdateStatement {
    UpdateTable(String, Vec<(String, DataValue)>, Option<WhereType>),
}

impl UpdateStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = delimited(
            multispace0,
            tag_no_case("UPDATE"),
            multispace1
        )(input)?;

        let (input, table_name) = delimited(
            multispace0,
            alphanumeric1,
            multispace0
        )(input)?;

        let (input, _) = delimited(
            multispace0,
            tag_no_case("SET"),
            multispace1
        )(input)?;

        let (input, updates) = separated_list1(
            delimited(multispace0, tag(","), multispace0),
            map(
                tuple((
                    alphanumeric1,
                    delimited(multispace0, tag("="), multispace0),
                    DataValue::parse
                )),
                |(col, _, val)| (col.to_string(), val)
            )
        )(input)?;

        let (input, where_clause) = opt(parse_where_clause)(input)?;

        Ok((input, Statement::Update(UpdateStatement::UpdateTable(
            table_name.to_string(),
            updates,
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
    fn parse_update_with_where_test() {
        let input = "UPDATE users SET name = 'John' WHERE id = 1";
        let (remaining, stmt) = UpdateStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, Some(WhereType::Regular(where_clause)))) => {
                assert_eq!(table_name, "users");
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].0, "name");
                assert_eq!(updates[0].1, DataValue::Text("John".to_string()));
                assert_eq!(where_clause.col_name, "id");
                assert_eq!(where_clause.operator, Op::Equal);
                assert_eq!(where_clause.value, DataValue::Integer(1));
            }
            _ => panic!("Expected Update statement with where clause"),
        }
    }

    #[test]
    fn parse_update_multiple_columns_test() {
        let input = "UPDATE users SET name = 'John', age = 30, status = 'active' WHERE status = 'active'";
        let (remaining, stmt) = UpdateStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, Some(WhereType::Regular(where_clause)))) => {
                assert_eq!(table_name, "users");
                assert_eq!(updates.len(), 3);
                assert_eq!(updates[0].0, "name");
                assert_eq!(updates[0].1, DataValue::Text("John".to_string()));
                assert_eq!(updates[1].0, "age");
                assert_eq!(updates[1].1, DataValue::Integer(30));
                assert_eq!(updates[2].0, "status");
                assert_eq!(updates[2].1, DataValue::Text("active".to_string()));
                assert_eq!(where_clause.col_name, "status");
                assert_eq!(where_clause.operator, Op::Equal);
                assert_eq!(where_clause.value, DataValue::Text("active".to_string()));
            }
            _ => panic!("Expected Update statement with where clause"),
        }
    }
}
