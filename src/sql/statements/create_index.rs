use nom::{
    bytes::complete::{tag_no_case, tag},
    character::complete::{multispace0, multispace1, alphanumeric1},
    combinator::opt,
    sequence::tuple,
    IResult,
};

use super::Statement;

#[derive(Debug, PartialEq, Clone)]
pub enum IndexType {
    BTree,
    GIN,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexStatement {
    pub table_name: String,
    pub column_name: String,
    pub index_type: IndexType,
}

impl CreateIndexStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("CREATE")(input)?;
        let (input, _) = multispace1(input)?;
        
        // Optional index type
        let (input, index_type) = opt(tuple((
            tag_no_case("GIN"),
            multispace1
        )))(input)?;
        
        let (input, _) = tag_no_case("INDEX")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("ON")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = tag("(")(input)?;
        let (input, _) = multispace0(input)?;
        let (input, column_name) = alphanumeric1(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = tag(")")(input)?;
        
        Ok((
            input,
            Statement::CreateIndex(CreateIndexStatement {
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
                index_type: if index_type.is_some() { IndexType::GIN } else { IndexType::BTree },
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_btree_index_parse() {
        let input = "CREATE INDEX ON users(id)";
        let (remaining, stmt) = CreateIndexStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::CreateIndex(stmt) => {
                assert_eq!(stmt.table_name, "users");
                assert_eq!(stmt.column_name, "id");
                assert_eq!(stmt.index_type, IndexType::BTree);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_create_gin_index_parse() {
        let input = "CREATE GIN INDEX ON articles(content)";
        let (remaining, stmt) = CreateIndexStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::CreateIndex(stmt) => {
                assert_eq!(stmt.table_name, "articles");
                assert_eq!(stmt.column_name, "content");
                assert_eq!(stmt.index_type, IndexType::GIN);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_create_gin_index_parse_with_whitespace() {
        let input = "CREATE GIN INDEX ON articles ( content )";
        let (remaining, stmt) = CreateIndexStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::CreateIndex(stmt) => {
                assert_eq!(stmt.table_name, "articles");
                assert_eq!(stmt.column_name, "content");
                assert_eq!(stmt.index_type, IndexType::GIN);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }
}