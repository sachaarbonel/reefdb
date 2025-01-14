use super::{Language, QueryType, TSQuery};
use crate::sql::column::Column;
use crate::sql::clauses::full_text_search::weight::TextWeight;
use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::{multispace0, anychar},
    sequence::{tuple, delimited},
    combinator::opt,
};
use crate::sql::operators::op::Op;

#[derive(Debug, Clone, PartialEq)]
pub struct FTSClause {
    pub column: Column,
    pub query: TSQuery,
    pub weight: Option<TextWeight>,
}

impl FTSClause {
    pub fn new(column: Column, query_text: String) -> Self {
        // Detect if we need to use Raw query type (when we have boolean operators)
        let query_type = if query_text.contains('&') || query_text.contains('|') || query_text.contains('!') {
            QueryType::Raw
        } else {
            QueryType::Plain
        };

        Self {
            column,
            query: TSQuery::new(query_text).with_type(query_type),
            weight: None,
        }
    }

    pub fn with_language(mut self, language: Language) -> Self {
        self.query = self.query.with_language(language);
        self
    }

    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query = self.query.with_type(query_type);
        self
    }

    pub fn with_weight(mut self, weight: TextWeight) -> Self {
        self.weight = Some(weight);
        self
    }

    pub fn parse_setweight(input: &str) -> IResult<&str, Self> {
        let (input, _) = tuple((
            tag_no_case("setweight"),
            multispace0,
            tag("("),
            multispace0,
        ))(input)?;

        // Parse inner tsvector
        let (input, inner_clause) = Self::parse_tsvector(input)?;

        let (input, _) = tuple((
            tag(","),
            multispace0,
            tag("'"),
        ))(input)?;

        // Parse weight character
        let (input, weight_char) = anychar(input)?;
        let weight = TextWeight::from_char(weight_char)
            .ok_or_else(|| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        let (input, _) = tuple((
            tag("'"),
            multispace0,
            tag(")"),
        ))(input)?;

        Ok((input, inner_clause.with_weight(weight)))
    }

    pub fn parse_tsvector(input: &str) -> IResult<&str, Self> {
        let (input, _) = tuple((
            tag_no_case("to_tsvector"),
            multispace0,
            tag("("),
        ))(input)?;

        // Parse optional language parameter
        let (input, language) = opt(tuple((
            delimited(
                tag("'"),
                tag_no_case("english"),
                tag("'"),
            ),
            tag(","),
            multispace0,
        )))(input)?;

        // Parse column
        let (input, column) = Column::parse(input)?;
        let (input, _) = tag(")")(input)?;

        let mut clause = FTSClause::new(column, String::new());
        if language.is_some() {
            clause = clause.with_language(Language::English);
        }

        Ok((input, clause))
    }

    pub fn parse(input: &str) -> IResult<&str, Self> {
        // Try parsing setweight first
        if let Ok((remaining, clause)) = Self::parse_setweight(input) {
            return Ok((remaining, clause));
        }

        // If not setweight, parse as regular tsvector
        let (input, clause) = Self::parse_tsvector(input)?;

        let (input, _) = tuple((
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

        let mut result = FTSClause::new(clause.column, query_text.to_string());
        
        // Only set language if it was explicitly specified in either tsvector or tsquery
        if query_language.is_some() || clause.query.language.is_some() {
            result = result.with_language(Language::English);
        }

        Ok((input, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fts_basic() {
        let input = "to_tsvector(content) @@ to_tsquery('web & development')";
        let (remaining, clause) = FTSClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "content");
        assert_eq!(clause.query.text, "web & development");
        assert_eq!(clause.query.language, None);
    }

    #[test]
    fn test_parse_fts_with_language() {
        let input = "to_tsvector('english', content) @@ to_tsquery('english', 'web & development')";
        let (remaining, clause) = FTSClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "content");
        assert_eq!(clause.query.text, "web & development");
        assert_eq!(clause.query.language, Some(Language::English));
    }

    #[test]
    fn test_parse_fts_with_table() {
        let input = "to_tsvector(posts.content) @@ to_tsquery('web & development')";
        let (remaining, clause) = FTSClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "content");
        assert_eq!(clause.column.table, Some("posts".to_string()));
        assert_eq!(clause.query.text, "web & development");
        assert_eq!(clause.query.language, None);
    }

    #[test]
    fn test_parse_setweight() {
        let input = "setweight(to_tsvector('english', title), 'A')";
        let (remaining, clause) = FTSClause::parse_setweight(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "title");
        assert_eq!(clause.weight, Some(TextWeight::A));
    }
} 