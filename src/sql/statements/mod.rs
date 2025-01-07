use self::{
    create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
    select::SelectStatement, update::UpdateStatement, alter::AlterStatement, drop::DropStatement,
    create_index::CreateIndexStatement, drop_index::DropIndexStatement,
};

use nom::{branch::alt, character::complete::multispace0, sequence::preceded, IResult};

pub mod create;
pub mod delete;
pub mod insert;
pub mod select;
pub mod update;
pub mod alter;
pub mod drop;
pub mod create_index;
pub mod drop_index;

#[derive(Debug, PartialEq)]
pub enum Statement {
    Create(CreateStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Alter(AlterStatement),
    Drop(DropStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
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
                CreateIndexStatement::parse,
                DropIndexStatement::parse,
            )),
        )(input)
    }
}
