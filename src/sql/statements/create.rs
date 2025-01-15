use crate::sql::column_def::{ColumnDef, table_name};
use nom::{
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    multi::separated_list1,
    sequence::{delimited, tuple, terminated},
    combinator::opt,
    IResult,
};

use super::Statement;

#[derive(Debug, PartialEq, Clone)]
pub enum CreateStatement {
    Table(String, Vec<ColumnDef>),
}

impl CreateStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("CREATE TABLE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = table_name(input)?;
        let (input, _) = multispace0(input)?;
        let (input, columns) = delimited(
            tag_no_case("("),
            separated_list1(
                tuple((multispace0, tag_no_case(","), multispace0)),
                ColumnDef::parse
            ),
            tuple((multispace0, opt(tuple((tag_no_case(","), multispace0))), tag_no_case(")"))),
        )(input)?;

        Ok((
            input,
            Statement::Create(CreateStatement::Table(table_name.to_string(), columns)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::{data_type::DataType, statements::Statement};

    //create full text search table
    #[test]
    fn parse_data_type_tsvector(){
        use super::CreateStatement;
        use crate::sql::column_def::ColumnDef;

        assert_eq!(
            CreateStatement::parse("CREATE TABLE users (id INTEGER, name TEXT, fts TSVECTOR)"),
            Ok((
                "",
                Statement::Create(CreateStatement::Table(
                    "users".to_string(),
                    vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Integer,

                            constraints: vec![],
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Text,
                            constraints: vec![],
                        },
                        ColumnDef {
                            name: "fts".to_string(),
                            data_type: DataType::TSVector,
                            constraints: vec![],
                        },
                    ]
                ))
            ))
        );
    }

    #[test]
    fn parse_test() {
        use super::CreateStatement;
        use crate::sql::column_def::ColumnDef;

        assert_eq!(
            CreateStatement::parse("CREATE TABLE users (id INTEGER, name TEXT)"),
            Ok((
                "",
                Statement::Create(CreateStatement::Table(
                    "users".to_string(),
                    vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Integer,

                            constraints: vec![],
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Text,
                            constraints: vec![],
                        },
                    ]
                ))
            ))
        );
    }
}
