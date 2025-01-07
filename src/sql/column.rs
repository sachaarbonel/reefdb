use nom::{IResult, character::complete::alphanumeric1, sequence::terminated, combinator::opt, bytes::complete::tag};

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
}

//nom parse tablename.columnname

impl Column {
    pub fn parse(input:&str)-> IResult<&str,Column>{
        let (input,table) = opt(terminated(alphanumeric1,tag(".")))(input)?;
        let (input,name) = alphanumeric1(input)?;
        Ok((input,Column{
            name:name.to_string(),
            table:table.map(|s|s.to_string()),
        }))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parser_test() {
        use super::Column;
        assert_eq!(Column::parse("id"),Ok(("",Column{name:"id".to_string(),table:None})));
        assert_eq!(Column::parse("users.id"),Ok(("",Column{name:"id".to_string(),table:Some("users".to_string())})));
    }
}
