use crate::sql::data_value::DataValue;
use crate::sql::column_def::table_name;

use nom::{
    bytes::complete::{tag, tag_no_case},
    character::complete::{alphanumeric1, multispace0, multispace1},
    multi::separated_list0,
    sequence::{delimited, tuple},
    IResult,
    combinator::opt,
};

use super::Statement;

#[derive(Debug, PartialEq, Clone)]
pub enum InsertStatement {
    IntoTable(String, Vec<DataValue>),
}

impl InsertStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag_no_case("INSERT INTO")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = table_name(input)?;
        let (input, _) = multispace0(input)?;
        
        // Optional column names
        let (input, _) = opt(tuple((
            tag("("),
            multispace0,
            separated_list0(
                tuple((multispace0, tag(","), multispace0)),
                alphanumeric1
            ),
            multispace0,
            tag(")")
        )))(input)?;
        
        let (input, _) = multispace0(input)?;
        let (input, _) = tag_no_case("VALUES")(input)?;
        let (input, _) = multispace0(input)?;
        let (input, values) = delimited(
            tag("("),
            separated_list0(
                tuple((multispace0, tag(","), multispace0)),
                DataValue::parse
            ),
            tag(")")
        )(input)?;

        let values: Vec<DataValue> = values.into_iter().collect();

        Ok((
            input,
            Statement::Insert(InsertStatement::IntoTable(table_name.to_string(), values)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::statements::Statement;

    #[test]
    fn parse_insert_with_columns() {
        let input = "INSERT INTO users(id,name) VALUES(1,'Alice')";
        let (remaining, stmt) = InsertStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                assert_eq!(table_name, "users");
                assert_eq!(values, vec![
                    DataValue::Integer(1),
                    DataValue::Text("Alice".to_string()),
                ]);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn parse_insert_without_columns() {
        let input = "INSERT INTO users VALUES(1,'Alice')";
        let (remaining, stmt) = InsertStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                assert_eq!(table_name, "users");
                assert_eq!(values, vec![
                    DataValue::Integer(1),
                    DataValue::Text("Alice".to_string()),
                ]);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn parse_insert_with_escaped_quotes() {
        let input = "INSERT INTO articles(id,title,content,language) VALUES(1,'Rust''s Guide','Learn Rust''s features','english')";
        let (remaining, stmt) = InsertStatement::parse(input).unwrap();
        assert_eq!(remaining, "");
        match stmt {
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                assert_eq!(table_name, "articles");
                assert_eq!(values, vec![
                    DataValue::Integer(1),
                    DataValue::Text("Rust's Guide".to_string()),
                    DataValue::Text("Learn Rust's features".to_string()),
                    DataValue::Text("english".to_string()),
                ]);
            }
            _ => panic!("Expected Insert statement"),
        }
    }
}
