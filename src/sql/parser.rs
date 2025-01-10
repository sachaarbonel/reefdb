use nom::IResult;
use crate::sql::statements::Statement;
use crate::error::ReefDBError;

pub struct Parser;

impl Parser {
    pub fn parse_sql(input: &str) -> Result<Statement, ReefDBError> {
        match Statement::parse(input) {
            Ok((remaining, stmt)) => {
                if remaining.trim().is_empty() {
                    Ok(stmt)
                } else {
                    Err(ReefDBError::Other(format!("Unexpected input after statement: {}", remaining)))
                }
            }
            Err(e) => Err(ReefDBError::Other(format!("Failed to parse SQL: {}", e))),
        }
    }
} 