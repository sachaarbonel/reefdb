use crate::sql::clauses::full_text_search::clause::FTSClause;
use crate::sql::clauses::full_text_search::language::Language;
use crate::sql::column::Column;
use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case},
    character::complete::multispace0,
    sequence::tuple,
    combinator::opt,
};

#[derive(Debug, Clone, PartialEq)]
pub struct TSVector {
    pub column: Column,
    pub language: Option<Language>,
}

impl TSVector {
    pub fn parse(input: &str) -> IResult<&str, FTSClause> {
        let (input, _) = tuple((
            tag_no_case("to_tsvector"),
            multispace0,
            tag("("),
        ))(input)?;

        // Parse optional language parameter
        let (input, language) = opt(tuple((
            Language::parse,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tsvector_basic() {
        let input = "to_tsvector(content)";
        let (remaining, clause) = TSVector::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "content");
        assert_eq!(clause.query.language, None);
    }

    #[test]
    fn test_parse_tsvector_with_language() {
        let input = "to_tsvector('english', content)";
        let (remaining, clause) = TSVector::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "content");
        assert_eq!(clause.query.language, Some(Language::English));
    }

    #[test]
    fn test_parse_tsvector_with_table() {
        let input = "to_tsvector(posts.content)";
        let (remaining, clause) = TSVector::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "content");
        assert_eq!(clause.column.table, Some("posts".to_string()));
        assert_eq!(clause.query.language, None);
    }
} 