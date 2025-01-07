use nom::{
    bytes::complete::{tag, take_while1},
    combinator::opt,
    sequence::tuple,
    IResult,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnValuePair {
    pub column_name: String,
    pub table_name: String,
}

impl ColumnValuePair {
    pub fn new(column_name: &str, table_name: &str) -> Self {
        Self {
            column_name: column_name.to_string(),
            table_name: table_name.to_string(),
        }
    }
}

pub fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

impl ColumnValuePair {
    pub fn parse(input: &str) -> IResult<&str, ColumnValuePair> {
        let (input, (table_part, column)) =
            tuple((opt(tuple((identifier, tag(".")))), identifier))(input)?;

        let table_name = table_part
            .map(|(table, _)| table.to_string())
            .unwrap_or_default();

        Ok((
            input,
            ColumnValuePair {
                column_name: column.to_string(),
                table_name,
            },
        ))
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn parser_test() {
        use super::ColumnValuePair;

        let input = "id";
        let expected = ColumnValuePair::new("id", "");
        let actual = ColumnValuePair::parse(input).unwrap().1;
        assert_eq!(expected, actual);

        let input = "users.id";
        let expected = ColumnValuePair::new("id", "users");
        let actual = ColumnValuePair::parse(input).unwrap().1;
        assert_eq!(expected, actual);
    }
}
