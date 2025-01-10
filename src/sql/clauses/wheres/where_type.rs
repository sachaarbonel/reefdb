use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case, take_till},
    character::complete::{multispace0, multispace1},
    sequence::{tuple, delimited},
    branch::alt,
    combinator::{opt, map},
};
use crate::sql::{
    column::Column,
    data_value::DataValue,
    operators::op::Op,
};

#[derive(Debug, PartialEq, Clone)]
pub enum WhereType {
    Regular(WhereClause),
    FTS(FTSWhereClause),
    And(Box<WhereType>, Box<WhereType>),
    Or(Box<WhereType>, Box<WhereType>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub col_name: String,
    pub operator: Op,
    pub value: DataValue,
    pub table: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FTSWhereClause {
    pub col: Column,
    pub query: String,
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
        let (input, _) = multispace1(input)?;
        let (input, operator) = Op::parse(input)?;
        let (input, _) = multispace1(input)?;
        let (input, value) = DataValue::parse(input)?;

        Ok((input, WhereClause {
            col_name: col.name,
            operator,
            value,
            table: col.table,
        }))
    }

    pub fn evaluate(&self, row_value: &DataValue) -> bool {
        self.operator.evaluate(row_value, &self.value)
    }
}

impl FTSWhereClause {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (input, _) = tag_no_case("MATCH")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, col) = Column::parse(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("AGAINST")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, query) = delimited(
            tag("'"),
            take_till(|c| c == '\''),
            tag("'")
        )(input)?;

        Ok((input, FTSWhereClause {
            col,
            query: query.to_string(),
        }))
    }
}

pub fn parse_where_clause(input: &str) -> IResult<&str, WhereType> {
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    parse_where_expression(input)
}

fn parse_where_expression(input: &str) -> IResult<&str, WhereType> {
    let (input, first) = parse_single_clause(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, rest) = opt(alt((
        // Parse AND condition
        map(
            tuple((
                tag_no_case("AND"),
                multispace1,
                parse_where_expression,
            )),
            |(_, _, right)| WhereType::And(Box::new(first.clone()), Box::new(right))
        ),
        // Parse OR condition
        map(
            tuple((
                tag_no_case("OR"),
                multispace1,
                parse_where_expression,
            )),
            |(_, _, right)| WhereType::Or(Box::new(first.clone()), Box::new(right))
        ),
    )))(input)?;

    Ok((input, rest.unwrap_or(first)))
}

fn parse_single_clause(input: &str) -> IResult<&str, WhereType> {
    let (input, clause) = alt((
        // Parse parenthesized expression
        map(
            tuple((
                tag("("),
                multispace0,
                parse_where_expression,
                multispace0,
                tag(")"),
            )),
            |(_, _, expr, _, _)| expr
        ),
        // Parse FTS clause
        map(FTSWhereClause::parse, WhereType::FTS),
        // Parse regular clause
        map(WhereClause::parse, WhereType::Regular),
    ))(input)?;

    Ok((input, clause))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_regular_where_clause() {
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
    }

    #[test]
    fn test_parse_regular_where_clause_with_table() {
        let input = "WHERE users.age > 18";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Regular(clause) => {
                assert_eq!(clause.col_name, "age");
                assert_eq!(clause.operator, Op::GreaterThan);
                assert_eq!(clause.value, DataValue::Integer(18));
                assert_eq!(clause.table, Some("users".to_string()));
            }
            _ => panic!("Expected Regular where clause"),
        }
    }

    #[test]
    fn test_parse_fts_clause() {
        let input = "WHERE MATCH description AGAINST 'computer science'";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        
        assert_eq!(remaining, "");
        match where_type {
            WhereType::FTS(clause) => {
                assert_eq!(clause.col.name, "description");
                assert_eq!(clause.col.table, None);
                assert_eq!(clause.query, "computer science");
            }
            _ => panic!("Expected FTS where clause"),
        }
    }

    #[test]
    fn test_parse_fts_clause_with_table() {
        let input = "WHERE MATCH books.description AGAINST 'computer science'";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        
        assert_eq!(remaining, "");
        match where_type {
            WhereType::FTS(clause) => {
                assert_eq!(clause.col.name, "description");
                assert_eq!(clause.col.table, Some("books".to_string()));
                assert_eq!(clause.query, "computer science");
            }
            _ => panic!("Expected FTS where clause"),
        }
    }

    #[test]
    fn test_parse_fts_clause_with_invalid_query() {
        let input = "WHERE MATCH description AGAINST 123";
        assert!(parse_where_clause(input).is_err());
    }

    #[test]
    fn test_parse_complex_and_condition() {
        let input = "WHERE age > 18 AND country = 'USA'";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        
        assert_eq!(remaining, "");
        match where_type {
            WhereType::And(left, right) => {
                match (*left, *right) {
                    (WhereType::Regular(left_clause), WhereType::Regular(right_clause)) => {
                        assert_eq!(left_clause.col_name, "age");
                        assert_eq!(left_clause.operator, Op::GreaterThan);
                        assert_eq!(left_clause.value, DataValue::Integer(18));
                        assert_eq!(left_clause.table, None);

                        assert_eq!(right_clause.col_name, "country");
                        assert_eq!(right_clause.operator, Op::Equal);
                        assert_eq!(right_clause.value, DataValue::Text("USA".to_string()));
                        assert_eq!(right_clause.table, None);
                    }
                    _ => panic!("Expected Regular clauses inside AND"),
                }
            }
            _ => panic!("Expected AND condition"),
        }
    }

    #[test]
    fn test_parse_complex_or_condition() {
        let input = "WHERE age > 18 OR country = 'USA'";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Or(left, right) => {
                match (*left, *right) {
                    (WhereType::Regular(left_clause), WhereType::Regular(right_clause)) => {
                        assert_eq!(left_clause.col_name, "age");
                        assert_eq!(left_clause.operator, Op::GreaterThan);
                        assert_eq!(left_clause.value, DataValue::Integer(18));
                        assert_eq!(left_clause.table, None);

                        assert_eq!(right_clause.col_name, "country");
                        assert_eq!(right_clause.operator, Op::Equal);
                        assert_eq!(right_clause.value, DataValue::Text("USA".to_string()));
                        assert_eq!(right_clause.table, None);
                    }
                    _ => panic!("Expected Regular clauses inside OR"),
                }
            }
            _ => panic!("Expected OR condition"),
        }
    }

    #[test]
    fn test_parse_parenthesized_condition() {
        let input = "WHERE (age > 18 AND country = 'USA') OR city = 'NYC'";
        let (remaining, where_type) = parse_where_clause(input).unwrap();
        
        assert_eq!(remaining, "");
        match where_type {
            WhereType::Or(left, right) => {
                match (*left, *right) {
                    (WhereType::And(and_left, and_right), WhereType::Regular(right_clause)) => {
                        match (*and_left, *and_right) {
                            (WhereType::Regular(age_clause), WhereType::Regular(country_clause)) => {
                                assert_eq!(age_clause.col_name, "age");
                                assert_eq!(age_clause.operator, Op::GreaterThan);
                                assert_eq!(age_clause.value, DataValue::Integer(18));
                                assert_eq!(age_clause.table, None);

                                assert_eq!(country_clause.col_name, "country");
                                assert_eq!(country_clause.operator, Op::Equal);
                                assert_eq!(country_clause.value, DataValue::Text("USA".to_string()));
                                assert_eq!(country_clause.table, None);
                            }
                            _ => panic!("Expected Regular clauses inside AND"),
                        }

                        assert_eq!(right_clause.col_name, "city");
                        assert_eq!(right_clause.operator, Op::Equal);
                        assert_eq!(right_clause.value, DataValue::Text("NYC".to_string()));
                        assert_eq!(right_clause.table, None);
                    }
                    _ => panic!("Expected AND and Regular clauses inside OR"),
                }
            }
            _ => panic!("Expected OR condition"),
        }
    }
}
