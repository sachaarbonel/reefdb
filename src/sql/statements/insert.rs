use crate::sql::data_value::DataValue;

use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, multispace1},
    multi::separated_list0,
    sequence::{delimited, terminated},
    IResult,
};

use super::Statement;

#[derive(Debug, PartialEq, Clone)]
pub enum InsertStatement {
    IntoTable(String, Vec<DataValue>),
}

impl InsertStatement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = tag("INSERT INTO")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table_name) = alphanumeric1(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("VALUES")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, values) = delimited(
            tag("("),
            separated_list0(terminated(tag(","), multispace0), DataValue::parse),
            tag(")"),
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
    use crate::sql::{data_value::DataValue, statements::insert::InsertStatement};

    #[test]
    fn parse_3_test() {
        let input = "INSERT INTO users VALUES (1, 'Alice')";
       
        assert_eq!(
            InsertStatement::parse(input),
            Ok((
                "",
                super::Statement::Insert(InsertStatement::IntoTable(
                    "users".to_string(),
                    vec![
                        DataValue::Integer(1),
                        DataValue::Text("Alice".to_string())
                    ]
                ))
            ))
        );
    }
    #[test]
    fn parse_2_test() {
        use crate::sql::data_value::DataValue;
        use crate::sql::statements::Statement;

        assert_eq!(
            InsertStatement::parse("INSERT INTO users VALUES (1, 'Alice')"),
            Ok((
                "",
                Statement::Insert(InsertStatement::IntoTable(
                    "users".to_string(),
                    vec![DataValue::Integer(1), DataValue::Text("Alice".to_string())]
                ))
            ))
        );
    }
}
