use self::{
    create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
    select::SelectStatement, update::UpdateStatement,
};

use nom::{branch::alt, character::complete::multispace0, sequence::preceded, IResult};

pub mod create;
pub mod delete;
pub mod insert;
pub mod select;
pub mod update;
pub mod constraints;

#[derive(Debug)]
pub enum Statement {
    Create(CreateStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl Statement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        preceded(
            multispace0,
            alt((
                CreateStatement::parse,
                InsertStatement::parse,
                SelectStatement::parse,
                UpdateStatement::parse,
                DeleteStatement::parse,
            )),
        )(input)
    }
}
