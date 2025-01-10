use nom::{
    bytes::complete::{tag, tag_no_case, take_till},
    character::complete::{alphanumeric1, multispace1, space1},
    sequence::delimited,
    IResult,
};

use crate::sql::{
    column::Column, column_def::column_name, data_value::DataValue, operators::op::Op,
};

#[derive(Debug, PartialEq, Clone)]
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

    pub fn evaluate(&self, row_value: &DataValue) -> bool {
        self.operator.evaluate(row_value, &self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn where_clause_test() {
        let clause = WhereClause::new(
            "age".to_string(),
            Op::GreaterThan,
            DataValue::Integer(18),
            None,
        );
        assert!(clause.evaluate(&DataValue::Integer(20)));
        assert!(!clause.evaluate(&DataValue::Integer(16)));
    }
}
