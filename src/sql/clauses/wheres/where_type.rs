use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::{multispace0, multispace1},
    sequence::{tuple, delimited},
    combinator::{map, opt},
};

use crate::sql::{
    column::Column,
    data_value::DataValue,
    operators::op::Op,
    clauses::full_text_search::{
        FTSClause,
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
    alt((
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
        map(FTSClause::parse, WhereType::FTS),
        // Parse regular clause
        map(WhereClause::parse, WhereType::Regular),
    ))(input)
}

pub fn parse_fts_where(input: &str) -> IResult<&str, WhereType> {
    let (input, _) = tag_no_case("to_tsvector")(input)?;
    let (input, _) = tag("(")(input)?;

    // Parse optional language
    let (input, language) = opt(tuple((
        delimited(
            tag("'"),
            tag_no_case("english"),
            tag("'"),
        ),
        tag(","),
        multispace0,
    )))(input)?;

    let (input, col) = Column::parse(input)?;

    let (input, _) = tuple((
        tag(")"),
        multispace0,
        |i| Op::parse(i).map(|(i, _)| (i, ())), // Parse @@ operator
        multispace0,
        tag_no_case("to_tsquery"),
        tag("("),
    ))(input)?;

    // Parse optional language for query
    let (input, query_language) = opt(tuple((
        delimited(
            tag("'"),
            tag_no_case("english"),
            tag("'"),
        ),
        tag(","),
        multispace0,
    )))(input)?;

    // Parse search query
    let (input, query_text) = delimited(
        tag("'"),
        take_until("'"),
        tag("'"),
    )(input)?;

    let (input, _) = tag(")")(input)?;

    let mut query = TSQuery::new(query_text.to_string());
    
    // Only set language if it was explicitly specified in either tsvector or tsquery
    if language.is_some() || query_language.is_some() {
        query = query.with_language(Language::English);
    }

    // Detect if we need to use Raw query type (when we have boolean operators)
    let query_type = if query_text.contains('&') || query_text.contains('|') || query_text.contains('!') {
        QueryType::Raw
    } else {
        QueryType::Plain
    };

    Ok((input, WhereType::FTS(FTSClause::new(col, query.text)
        .with_query_type(query_type)
        .with_language(query.language.unwrap_or_default()))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::full_text_search::{TSQuery, QueryType, Language};

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
}
