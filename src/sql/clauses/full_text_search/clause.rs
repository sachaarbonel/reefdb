use super::{QueryType, TSQuery};
use super::language::Language;
use crate::sql::column::Column;
use crate::sql::clauses::full_text_search::weight::TextWeight;
use super::{set_weight::SetWeight, ts_vector::TSVector};
use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::multispace0,
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

    fn parse_tsquery(input: &str) -> IResult<&str, (Option<Language>, String)> {
        let (input, _) = tuple((
            tag_no_case("to_tsquery"),
            tag("("),
        ))(input)?;

        // Parse optional language
        let (input, language) = opt(tuple((
            Language::parse,
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

        Ok((input, (language.map(|(lang, _, _)| lang), query_text.to_string())))
    }

    pub fn parse(input: &str) -> IResult<&str, Self> {
        // Try parsing setweight first
        if let Ok((remaining, clause)) = SetWeight::parse(input) {
            return Ok((remaining, clause));
        }

        // Parse tsvector part
        let (input, clause) = TSVector::parse(input)?;

        // Parse @@ operator
        let (input, _) = tuple((
            multispace0,
            |i| Op::parse(i).map(|(i, _)| (i, ())),
            multispace0,
        ))(input)?;

        // Parse tsquery part
        let (input, (query_language, query_text)) = Self::parse_tsquery(input)?;

        let mut result = FTSClause::new(clause.column, query_text);
        
        // Set language if it was specified in either tsvector or tsquery
        if let Some(lang) = query_language.or(clause.query.language) {
            result = result.with_language(lang);
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
} 