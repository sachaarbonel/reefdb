use nom::{branch::alt, IResult, combinator::map, bytes::complete::{tag_no_case, tag}};

#[derive(Debug, PartialEq, Clone)]
pub enum Op {
    Match,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    TextSearch,
}

impl Op {
    pub fn parse(input: &str) -> IResult<&str, Op> {
        alt((
            map(tag("@@"), |_| Op::TextSearch),
            map(tag_no_case(">="), |_| Op::GreaterThanOrEqual),
            map(tag_no_case("<="), |_| Op::LessThanOrEqual),
            map(tag_no_case(">"), |_| Op::GreaterThan),
            map(tag_no_case("<"), |_| Op::LessThan),
            map(tag_no_case("="), |_| Op::Equal),
            map(tag_no_case("MATCH"), |_| Op::Match),
            map(tag_no_case("!="), |_| Op::NotEqual),
        ))(input)
    }

    pub fn evaluate(&self, left: &crate::sql::data_value::DataValue, right: &crate::sql::data_value::DataValue) -> bool {
        match self {
            Op::Equal => left == right,
            Op::NotEqual => left != right,
            Op::GreaterThan => left > right,
            Op::LessThan => left < right,
            Op::GreaterThanOrEqual => left >= right,
            Op::LessThanOrEqual => left <= right,
            Op::Match => false, // FTS matching is handled separately
            Op::TextSearch => false, // Full-text search matching is handled separately in the FTS module
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Op;
    use crate::sql::data_value::DataValue;

    #[test]
    fn parse_test() {
        assert_eq!(Op::parse("="), Ok(("", Op::Equal)));
        assert_eq!(Op::parse("MATCH"), Ok(("", Op::Match)));
        assert_eq!(Op::parse("!="), Ok(("", Op::NotEqual)));
        assert_eq!(Op::parse(">"), Ok(("", Op::GreaterThan)));
        assert_eq!(Op::parse("<"), Ok(("", Op::LessThan)));
        assert_eq!(Op::parse(">="), Ok(("", Op::GreaterThanOrEqual)));
        assert_eq!(Op::parse("<="), Ok(("", Op::LessThanOrEqual)));
        assert_eq!(Op::parse("@@"), Ok(("", Op::TextSearch)));
    }

    #[test]
    fn evaluate_test() {
        let op = Op::GreaterThan;
        assert!(op.evaluate(&DataValue::Integer(5), &DataValue::Integer(3)));
        assert!(!op.evaluate(&DataValue::Integer(3), &DataValue::Integer(5)));
        assert!(!op.evaluate(&DataValue::Integer(3), &DataValue::Integer(3)));

        let op = Op::GreaterThanOrEqual;
        assert!(op.evaluate(&DataValue::Integer(5), &DataValue::Integer(3)));
        assert!(op.evaluate(&DataValue::Integer(3), &DataValue::Integer(3)));
        assert!(!op.evaluate(&DataValue::Integer(3), &DataValue::Integer(5)));
    }
}
