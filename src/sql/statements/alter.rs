use nom::{
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{multispace1, alphanumeric1},
    combinator::map,
    sequence::tuple,
    IResult,
};

use crate::sql::column_def::ColumnDef;
use super::Statement;

#[derive(Debug, PartialEq)]
pub enum AlterType {
    AddColumn(ColumnDef),
    DropColumn(String),
    RenameColumn(String, String),
}

#[derive(Debug, PartialEq)]
pub struct AlterStatement {
    pub table_name: String,
    pub alter_type: AlterType,
}

impl AlterStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("ALTER TABLE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        
        let (input, alter_type) = alt((
            parse_add_column,
            parse_drop_column,
            parse_rename_column,
        ))(input)?;

        Ok((
            input,
            Statement::Alter(AlterStatement {
                table_name: table_name.to_string(),
                alter_type,
            }),
        ))
    }
}

fn parse_add_column(input: &str) -> IResult<&str, AlterType> {
    let (input, _) = tag_no_case("ADD COLUMN")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, column_def) = ColumnDef::parse(input)?;
    
    Ok((input, AlterType::AddColumn(column_def)))
}

fn parse_drop_column(input: &str) -> IResult<&str, AlterType> {
    let (input, _) = tag_no_case("DROP COLUMN")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, column_name) = alphanumeric1(input)?;
    
    Ok((input, AlterType::DropColumn(column_name.to_string())))
}

fn parse_rename_column(input: &str) -> IResult<&str, AlterType> {
    let (input, _) = tag_no_case("RENAME COLUMN")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, old_name) = alphanumeric1(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, new_name) = alphanumeric1(input)?;
    
    Ok((input, AlterType::RenameColumn(old_name.to_string(), new_name.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::data_type::DataType;

    #[test]
    fn test_alter_add_column() {
        assert_eq!(
            AlterStatement::parse("ALTER TABLE users ADD COLUMN age INTEGER"),
            Ok((
                "",
                Statement::Alter(AlterStatement {
                    table_name: "users".to_string(),
                    alter_type: AlterType::AddColumn(ColumnDef {
                        name: "age".to_string(),
                        data_type: DataType::Integer,
                        constraints: vec![],
                    }),
                })
            ))
        );
    }

    #[test]
    fn test_alter_drop_column() {
        assert_eq!(
            AlterStatement::parse("ALTER TABLE users DROP COLUMN age"),
            Ok((
                "",
                Statement::Alter(AlterStatement {
                    table_name: "users".to_string(),
                    alter_type: AlterType::DropColumn("age".to_string()),
                })
            ))
        );
    }

    #[test]
    fn test_alter_rename_column() {
        assert_eq!(
            AlterStatement::parse("ALTER TABLE users RENAME COLUMN username TO login"),
            Ok((
                "",
                Statement::Alter(AlterStatement {
                    table_name: "users".to_string(),
                    alter_type: AlterType::RenameColumn(
                        "username".to_string(),
                        "login".to_string()
                    ),
                })
            ))
        );
    }
}