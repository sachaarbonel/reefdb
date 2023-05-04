use nom::{
    bytes::complete::{tag, tag_no_case, take_till},
    character::complete::space1,
    sequence::delimited,
    IResult,
};

use crate::sql::{column::Column, data_value::DataValue, operators::op::Op};

use super::{fts::FTSWhereClause, where_clause::WhereClause};

#[derive(Debug, PartialEq)]
pub enum WhereType {
    Regular(WhereClause),
    FTS(FTSWhereClause),
}

impl WhereType {
    pub fn parse(input: &str) -> IResult<&str, WhereType> {
        let (input, _) = tag_no_case("WHERE")(input)?;
        let (input, _) = space1(input)?;
        let (input, col) = Column::parse(input)?;
        let (input, _) = space1(input)?;
        let (input, op) = Op::parse(input)?;
        let (input, _) = space1(input)?;

        let (input, where_type) = match op {
            Op::Match => {
                let (input, query) =
                    delimited(tag("'"), take_till(|c| c == '\''), tag("'"))(input)?;
                (
                    input,
                    WhereType::FTS(FTSWhereClause {
                        col: Column {
                            name: col.name,
                            // alias: col.alias,
                            table: col.table,
                        },
                        query: query.to_string(),
                    }),
                )
            }
            _ => {
                let (input, value) = DataValue::parse(input)?;
                (
                    input,
                    WhereType::Regular(WhereClause {
                        col_name: col.name,
                        value: value,
                    }),
                )
            }
        };

        Ok((input, where_type))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn where_type_fts_test() {
        use super::*;

        let res = WhereType::parse("WHERE name MATCH 'hello'");
        assert_eq!(
            res,
            Ok((
                "",
                WhereType::FTS(FTSWhereClause {
                    col: Column {
                        name: "name".to_string(),
                        // alias: None,
                        table: None,
                    },
                    query: "hello".to_string(),
                })
            ))
        );
    }
}
