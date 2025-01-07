use self::{
    create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
    select::SelectStatement, update::UpdateStatement, alter::AlterStatement, drop::DropStatement,
};

use nom::{branch::alt, character::complete::multispace0, sequence::preceded, IResult};

pub mod create;
pub mod delete;
pub mod insert;
pub mod select;
pub mod update;
pub mod alter;
pub mod drop;

#[derive(Debug, PartialEq)]
pub enum Statement {
    Create(CreateStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Alter(AlterStatement),
    Drop(DropStatement),
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
                AlterStatement::parse,
                DropStatement::parse,
            )),
        )(input)
    }
}
