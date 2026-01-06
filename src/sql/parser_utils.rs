use nom::{
    branch::alt,
    bytes::complete::tag,
    bytes::complete::tag_no_case,
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::recognize,
    multi::{many0, separated_list1},
    sequence::{delimited, tuple},
    IResult,
};

pub type SqlResult<'a, T> = IResult<&'a str, T, nom::error::Error<&'a str>>;

fn ident_raw(input: &str) -> SqlResult<&str> {
    recognize(tuple((
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    )))(input)
}

/// Standard SQL identifier (alpha/underscore followed by alnum/underscore).
pub fn ident(input: &str) -> SqlResult<&str> {
    ident_raw(input)
}

/// Identifier that allows dotted segments (table.column).
pub fn ident_allow_dot(input: &str) -> SqlResult<&str> {
    recognize(tuple((ident_raw, many0(tuple((tag("."), ident_raw))))))(input)
}

/// Identifier with surrounding whitespace trimmed.
pub fn ident_ws(input: &str) -> SqlResult<&str> {
    delimited(multispace0, ident_raw, multispace0)(input)
}

/// Case-insensitive keyword parser.
pub fn kw<'a>(keyword: &'static str) -> impl FnMut(&'a str) -> SqlResult<'a, &'a str> {
    tag_no_case(keyword)
}

/// Comma-separated list with optional surrounding whitespace.
pub fn comma_sep<'a, O, F>(parser: F) -> impl FnMut(&'a str) -> SqlResult<'a, Vec<O>>
where
    F: FnMut(&'a str) -> SqlResult<'a, O>,
{
    separated_list1(delimited(multispace0, tag(","), multispace0), parser)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_accepts_valid_names() {
        assert_eq!(ident("foo").unwrap().1, "foo");
        assert_eq!(ident("_bar").unwrap().1, "_bar");
        assert_eq!(ident("a1_b2").unwrap().1, "a1_b2");
    }

    #[test]
    fn ident_rejects_leading_digit() {
        assert!(ident("1abc").is_err());
    }

    #[test]
    fn ident_allow_dot_accepts_dotted() {
        assert_eq!(ident_allow_dot("table.column").unwrap().1, "table.column");
        assert_eq!(ident_allow_dot("a.b_c").unwrap().1, "a.b_c");
    }

    #[test]
    fn ident_ws_trims_whitespace() {
        assert_eq!(ident_ws("  name \t").unwrap().1, "name");
    }

    #[test]
    fn comma_sep_parses_list() {
        let mut parser = comma_sep(ident);
        let result = parser("a, b, c").unwrap().1;
        assert_eq!(result, vec!["a", "b", "c"]);
    }
}
