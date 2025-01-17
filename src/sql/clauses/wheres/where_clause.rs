use nom::{
    IResult,
    sequence::{delimited, tuple},
    character::complete::multispace0,
    bytes::complete::{tag, tag_no_case},
    branch::alt,
    multi::many0,
    combinator::{opt, recognize},
};
use crate::sql::{
    column::Column,
    operators::op::Op,
    data_value::DataValue,
};

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub col_name: String,
    pub operator: Op,
    pub value: DataValue,
    pub table: Option<String>,
}

impl WhereClause {
    pub fn new(col_name: String, operator: Op, value: DataValue, table: Option<String>) -> Self {
        Self {
            col_name,
            operator,
            value,
            table,
        }
    }

    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (input, col) = delimited(
            multispace0,
            Column::parse,
            multispace0
        )(input)?;

        let (input, operator) = delimited(
            multispace0,
            Op::parse,
            multispace0
        )(input)?;

        let (input, value) = delimited(
            multispace0,
            DataValue::parse,
            multispace0
        )(input)?;

        Ok((input, WhereClause {
            col_name: col.name,
            operator,
            value,
            table: col.table,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::data_value::DataValue;
    use crate::sql::operators::op::Op;

    #[test]
    fn test_parse_where_clause() {
        // Test basic where clause
        let input = "age > 18";
        let (remaining, clause) = WhereClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.col_name, "age");
        assert_eq!(clause.operator, Op::GreaterThan);
        assert_eq!(clause.value, DataValue::Integer(18));
        assert_eq!(clause.table, None);

        // Test with table name
        let input = "users.age = 25";
        let (remaining, clause) = WhereClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.col_name, "age");
        assert_eq!(clause.operator, Op::Equal);
        assert_eq!(clause.value, DataValue::Integer(25));
        assert_eq!(clause.table, Some("users".to_string()));

        // Test with extra whitespace
        let input = "  users.age   =  25  ";
        let (remaining, clause) = WhereClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.col_name, "age");
        assert_eq!(clause.operator, Op::Equal);
        assert_eq!(clause.value, DataValue::Integer(25));
        assert_eq!(clause.table, Some("users".to_string()));
    }
}
