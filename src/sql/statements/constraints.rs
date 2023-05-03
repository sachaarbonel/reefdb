use nom::{bytes::complete::tag, combinator::map, branch::alt, IResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Constraint {
    NotNull,
    PrimaryKey,
    Unique,
    // You can add more constraints here as needed.
}

impl Constraint {
    pub fn parse(input: &str) -> IResult<&str, Constraint> {
        alt((
            map(tag("NOT NULL"), |_| Constraint::NotNull),
            map(tag("PRIMARY KEY"), |_| Constraint::PrimaryKey),
            map(tag("UNIQUE"), |_| Constraint::Unique),
        ))(input)
    }
}
