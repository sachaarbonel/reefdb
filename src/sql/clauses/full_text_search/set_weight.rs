use super::{FTSClause, TextWeight, TSVector};
use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, anychar},
    sequence::tuple,
};

#[derive(Debug, Clone, PartialEq)]
pub struct SetWeight {
    pub inner_clause: FTSClause,
    pub weight: TextWeight,
}

impl SetWeight {
    pub fn parse(input: &str) -> IResult<&str, FTSClause> {
        let (input, _) = tuple((
            tag_no_case("setweight"),
            multispace0,
            tag("("),
            multispace0,
        ))(input)?;

        // Parse inner tsvector
        let (input, inner_clause) = TSVector::parse(input)?;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::clauses::full_text_search::Language;

    #[test]
    fn test_parse_setweight() {
        let input = "setweight(to_tsvector('english', title), 'A')";
        let (remaining, clause) = SetWeight::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clause.column.name, "title");
        assert_eq!(clause.weight, Some(TextWeight::A));
        assert_eq!(clause.query.language, Some(Language::English));
    }
} 