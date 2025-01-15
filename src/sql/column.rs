use nom::{
    IResult,
    character::complete::{alphanumeric1, char},
    sequence::{terminated, pair},
    combinator::{opt, recognize},
    multi::many0,
    bytes::complete::tag,
    branch::alt,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
}

impl Column {
    pub fn parse(input: &str) -> IResult<&str, Column> {
        let identifier = |input| {
            recognize(
                pair(
                    alphanumeric1,
                    many0(alt((
                        alphanumeric1,
                        recognize(char('_')),
                    )))
                )
            )(input)
        };

        let (input, table) = opt(terminated(identifier, tag(".")))(input)?;
        let (input, name) = identifier(input)?;
        Ok((input, Column {
            name: name.to_string(),
            table: table.map(|s| s.to_string()),
        }))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parser_test() {
        use super::Column;
        assert_eq!(Column::parse("id"), Ok(("", Column { name: "id".to_string(), table: None })));
        assert_eq!(Column::parse("users.id"), Ok(("", Column { name: "id".to_string(), table: Some("users".to_string()) })));
        assert_eq!(Column::parse("int_col"), Ok(("", Column { name: "int_col".to_string(), table: None })));
        assert_eq!(Column::parse("test_table.int_col"), Ok(("", Column { name: "int_col".to_string(), table: Some("test_table".to_string()) })));
    }
}
