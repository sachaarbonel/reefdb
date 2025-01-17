use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::{multispace0, multispace1},
    sequence::{tuple, delimited},
    multi::many0,
    combinator::{map, opt},
};

use crate::sql::{
    column::Column,
    data_value::DataValue,
    operators::op::Op,
    clauses::full_text_search::{
        clause::FTSClause,
        TSQuery,
        QueryType,
        Language,
    },
};

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
    pub col_name: String,
    pub operator: Op,
    pub value: DataValue,
    pub table: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WhereType {
    Regular(WhereClause),
    FTS(FTSClause),
    And(Box<WhereType>, Box<WhereType>),
    Or(Box<WhereType>, Box<WhereType>),
}

impl WhereClause {
    pub fn new(col_name: String, operator: Op, value: DataValue, table: Option<String>) -> Self {
        WhereClause {
            col_name,
            operator,
            value,
            table,
        }
    }

    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (input, col) = Column::parse(input)?;
        let (input, operator) = delimited(
            multispace0,
            Op::parse,
            multispace0
        )(input)?;
        let (input, value) = DataValue::parse(input)?;

        Ok((input, WhereClause {
            col_name: col.name,
            operator,
            value,
            table: col.table,
        }))
    }
}

pub fn parse_where_clause(input: &str) -> IResult<&str, WhereType> {
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, result) = parse_where_expression(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, result))
}

fn parse_binary_op(input: &str) -> IResult<&str, &str> {
    delimited(
        multispace1,
        alt((
            tag_no_case("AND"),
            tag_no_case("OR"),
        )),
        multispace1
    )(input)
}

fn parse_parenthesized(input: &str) -> IResult<&str, WhereType> {
    let (input, _) = tag("(")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_where_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag(")")(input)?;
    Ok((input, expr))
}

fn parse_simple_where(input: &str) -> IResult<&str, WhereType> {
    alt((
        parse_parenthesized,
        parse_fts_where_clause,
        map(WhereClause::parse, WhereType::Regular)
    ))(input)
}

pub fn parse_where_expression(input: &str) -> IResult<&str, WhereType> {
    let (mut input, mut result) = parse_simple_where(input)?;

    while let Ok((new_input, op)) = parse_binary_op(input) {
        let (newer_input, right) = parse_simple_where(new_input)?;
        result = match op.to_uppercase().as_str() {
            "AND" => WhereType::And(Box::new(result), Box::new(right)),
            "OR" => WhereType::Or(Box::new(result), Box::new(right)),
            _ => unreachable!(),
        };
        input = newer_input;
    }

    Ok((input, result))
}

pub fn parse_fts_where_clause(input: &str) -> IResult<&str, WhereType> {
    let (input, clause) = FTSClause::parse(input)?;
    Ok((input, WhereType::FTS(clause)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_where_clause() {
        // Test basic where clause
        let input = "WHERE age > 18";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Regular(clause) => {
                assert_eq!(clause.col_name, "age");
                assert_eq!(clause.operator, Op::GreaterThan);
                assert_eq!(clause.value, DataValue::Integer(18));
                assert_eq!(clause.table, None);
            }
            _ => panic!("Expected Regular where clause"),
        }

        // Test with table name
        let input = "WHERE users.age = 25";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Regular(clause) => {
                assert_eq!(clause.col_name, "age");
                assert_eq!(clause.operator, Op::Equal);
                assert_eq!(clause.value, DataValue::Integer(25));
                assert_eq!(clause.table, Some("users".to_string()));
            }
            _ => panic!("Expected Regular where clause"),
        }

        // Test with extra whitespace
        let input = "WHERE   users.age   =  25  ";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Regular(clause) => {
                assert_eq!(clause.col_name, "age");
                assert_eq!(clause.operator, Op::Equal);
                assert_eq!(clause.value, DataValue::Integer(25));
                assert_eq!(clause.table, Some("users".to_string()));
            }
            _ => panic!("Expected Regular where clause"),
        }
    }

    #[test]
    fn test_parse_complex_where() {
        let input = "WHERE age > 18 AND status = 'active'";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::And(left, right) => {
                match (*left, *right) {
                    (WhereType::Regular(left_clause), WhereType::Regular(right_clause)) => {
                        assert_eq!(left_clause.col_name, "age");
                        assert_eq!(left_clause.operator, Op::GreaterThan);
                        assert_eq!(left_clause.value, DataValue::Integer(18));
                        assert_eq!(right_clause.col_name, "status");
                        assert_eq!(right_clause.operator, Op::Equal);
                        assert_eq!(right_clause.value, DataValue::Text("active".to_string()));
                    }
                    _ => panic!("Expected two Regular clauses"),
                }
            }
            _ => panic!("Expected AND clause"),
        }
    }

    #[test]
    fn test_parse_fts_where() {
        let input = "WHERE to_tsvector(content) @@ to_tsquery('web & development')";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::FTS(clause) => {
                assert_eq!(clause.column.name, "content");
                assert_eq!(clause.query.text, "web & development");
                assert_eq!(clause.query.language, None);
            }
            _ => panic!("Expected FTS clause"),
        }
    }

    #[test]
    fn test_parse_fts_where_with_language() {
        let input = "WHERE to_tsvector('english', content) @@ to_tsquery('english', 'web & development')";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::FTS(clause) => {
                assert_eq!(clause.column.name, "content");
                assert_eq!(clause.query.text, "web & development");
                assert_eq!(clause.query.language, Some(Language::English));
            }
            _ => panic!("Expected FTS clause"),
        }
    }

    #[test]
    fn test_parse_complex_where_with_fts() {
        let input = "WHERE age > 18 AND to_tsvector(content) @@ to_tsquery('web & development')";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::And(left, right) => {
                match (*left, *right) {
                    (WhereType::Regular(left_clause), WhereType::FTS(right_clause)) => {
                        assert_eq!(left_clause.col_name, "age");
                        assert_eq!(left_clause.operator, Op::GreaterThan);
                        assert_eq!(left_clause.value, DataValue::Integer(18));
                        assert_eq!(right_clause.column.name, "content");
                        assert_eq!(right_clause.query.text, "web & development");
                    }
                    _ => panic!("Expected Regular and FTS clauses"),
                }
            }
            _ => panic!("Expected AND clause"),
        }
    }

    #[test]
    fn test_parse_complex_where_with_parentheses() {
        let input = "WHERE (age > 35 AND status = 'active') OR (country = 'UK' AND year = 2021)";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Or(left, right) => {
                match (*left, *right) {
                    (WhereType::And(left_and1, right_and1), WhereType::And(left_and2, right_and2)) => {
                        // Check first AND condition
                        match (*left_and1, *right_and1) {
                            (WhereType::Regular(left_clause), WhereType::Regular(right_clause)) => {
                                assert_eq!(left_clause.col_name, "age");
                                assert_eq!(left_clause.operator, Op::GreaterThan);
                                assert_eq!(left_clause.value, DataValue::Integer(35));
                                assert_eq!(right_clause.col_name, "status");
                                assert_eq!(right_clause.operator, Op::Equal);
                                assert_eq!(right_clause.value, DataValue::Text("active".to_string()));
                            }
                            _ => panic!("Expected two Regular clauses in first AND"),
                        }
                        // Check second AND condition
                        match (*left_and2, *right_and2) {
                            (WhereType::Regular(left_clause), WhereType::Regular(right_clause)) => {
                                assert_eq!(left_clause.col_name, "country");
                                assert_eq!(left_clause.operator, Op::Equal);
                                assert_eq!(left_clause.value, DataValue::Text("UK".to_string()));
                                assert_eq!(right_clause.col_name, "year");
                                assert_eq!(right_clause.operator, Op::Equal);
                                assert_eq!(right_clause.value, DataValue::Integer(2021));
                            }
                            _ => panic!("Expected two Regular clauses in second AND"),
                        }
                    }
                    _ => panic!("Expected two AND clauses"),
                }
            }
            _ => panic!("Expected OR clause"),
        }
    }
}
