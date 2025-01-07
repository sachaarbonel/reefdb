use nom::{
    bytes::complete::tag_no_case,
    character::complete::{multispace1, alphanumeric1},
    IResult,
};

use super::Statement;

#[derive(Debug, PartialEq)]
pub struct CreateIndexStatement {
    pub table_name: String,
    pub column_name: String,
}

impl CreateIndexStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("CREATE INDEX ON")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("(")(input)?;
        let (input, column_name) = alphanumeric1(input)?;
        let (input, _) = tag_no_case(")")(input)?;
        
        Ok((
            input,
            Statement::CreateIndex(CreateIndexStatement {
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_index_parse() {
        assert_eq!(
            CreateIndexStatement::parse("CREATE INDEX ON users (id)"),
            Ok((
                "",
                Statement::CreateIndex(CreateIndexStatement {
                    table_name: "users".to_string(),
                    column_name: "id".to_string(),
                })
            ))
        );
    }
}