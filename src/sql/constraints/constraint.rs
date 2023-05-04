use nom::{branch::alt, bytes::complete::tag, combinator::map, IResult};
use serde::{Deserialize, Serialize};

use super::foreignkey::ForeignKeyConstraint;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Constraint {
    NotNull,
    PrimaryKey,
    Unique,
    ForeignKey(ForeignKeyConstraint),
    // You can add more constraints here as needed.
}

impl Constraint {
    pub fn parse(input: &str) -> IResult<&str, Constraint> {
        alt((
            map(tag("NOT NULL"), |_| Constraint::NotNull),
            map(tag("PRIMARY KEY"), |_| Constraint::PrimaryKey),
            map(tag("UNIQUE"), |_| Constraint::Unique),
            ForeignKeyConstraint::parse,
        ))(input)
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::constraints::foreignkey::ForeignKeyConstraint;

    #[test]
    fn parser_test() {
        use crate::sql::constraints::constraint::Constraint;

        assert_eq!(Constraint::parse("NOT NULL"), Ok(("", Constraint::NotNull)));
        assert_eq!(
            Constraint::parse("PRIMARY KEY"),
            Ok(("", Constraint::PrimaryKey))
        );
        assert_eq!(Constraint::parse("UNIQUE"), Ok(("", Constraint::Unique)));
        assert_eq!(
            Constraint::parse("FOREIGN KEY (id) REFERENCES users"),
            Ok((
                "",
                Constraint::ForeignKey(ForeignKeyConstraint {
                    table_name: "users".to_string(),
                    column_name: "id".to_string(),
                })
            ))
        );
    }
}
