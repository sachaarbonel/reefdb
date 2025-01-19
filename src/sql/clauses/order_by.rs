use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag_no_case, tag},
    character::complete::{multispace0, multispace1},
    sequence::{tuple, preceded},
    multi::separated_list1,
    combinator::{opt, map},
};

use crate::sql::column::Column;

#[derive(Debug, PartialEq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause {
    pub column: Column,
    pub direction: OrderDirection,
}

impl OrderByClause {
    pub fn parse(input: &str) -> IResult<&str, Vec<OrderByClause>> {
        preceded(
            tuple((
                tag_no_case("ORDER"),
                multispace1,
                tag_no_case("BY"),
                multispace1,
            )),
            separated_list1(
                tuple((multispace0, tag(","), multispace0)),
                parse_order_by_item
            )
        )(input)
    }
}

fn parse_order_by_item(input: &str) -> IResult<&str, OrderByClause> {
    let (input, column) = Column::parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, direction) = opt(alt((
        map(tag_no_case("DESC"), |_| OrderDirection::Desc),
        map(tag_no_case("ASC"), |_| OrderDirection::Asc),
    )))(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, OrderByClause {
        column,
        direction: direction.unwrap_or(OrderDirection::Asc),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column::ColumnType;

    #[test]
    fn test_parse_order_by_simple() {
        let input = "ORDER BY age DESC";
        let (remaining, clauses) = OrderByClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].column.name, "age");
        assert_eq!(clauses[0].column.column_type, ColumnType::Regular("age".to_string()));
        assert_eq!(clauses[0].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_parse_order_by_multiple() {
        let input = "ORDER BY age DESC, name ASC";
        let (remaining, clauses) = OrderByClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0].column.name, "age");
        assert_eq!(clauses[0].column.column_type, ColumnType::Regular("age".to_string()));
        assert_eq!(clauses[0].direction, OrderDirection::Desc);
        assert_eq!(clauses[1].column.name, "name");
        assert_eq!(clauses[1].column.column_type, ColumnType::Regular("name".to_string()));
        assert_eq!(clauses[1].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_parse_order_by_default_asc() {
        let input = "ORDER BY age";
        let (remaining, clauses) = OrderByClause::parse(input).unwrap();
        assert_eq!(remaining, "");
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].column.name, "age");
        assert_eq!(clauses[0].column.column_type, ColumnType::Regular("age".to_string()));
        assert_eq!(clauses[0].direction, OrderDirection::Asc);
    }
} 