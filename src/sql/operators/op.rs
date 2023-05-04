use nom::{branch::alt, IResult, combinator::map, bytes::complete::tag_no_case};

#[derive(Debug, PartialEq)]
pub enum Op {
    Match,
    Equal,
    NotEqual,
}

impl Op {
    pub fn parse(input: &str) -> IResult<&str, Op> {
        alt((
            map(tag_no_case("="), |_| Op::Equal),
            map(tag_no_case("MATCH"), |_| Op::Match),
            map(tag_no_case("!="), |_| Op::NotEqual),
            // Add more comparison operators as needed
        ))(input)
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn parse_test() {
        use super::Op;
        assert_eq!(Op::parse("="), Ok(("", Op::Equal)));
        assert_eq!(Op::parse("MATCH"), Ok(("", Op::Match)));
        assert_eq!(Op::parse("!="), Ok(("", Op::NotEqual)));
    }
}
