use self::{
    create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
    select::SelectStatement, update::UpdateStatement, alter::AlterStatement, drop::DropStatement,
    create_index::CreateIndexStatement, drop_index::DropIndexStatement,
};

use nom::{
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{multispace0, multispace1},
    IResult,
};
use crate::sql::parser_utils::ident;

pub mod create;
pub mod delete;
pub mod insert;
pub mod select;
pub mod update;
pub mod alter;
pub mod drop;
pub mod create_index;
pub mod drop_index;

#[derive(Debug, PartialEq, Clone)]
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
    Savepoint(SavepointStatement),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    BeginTransaction,
    Commit,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SavepointStatement {
    pub name: String,
}

fn parse_begin_transaction(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("BEGIN TRANSACTION")(input)?;
    Ok((input, Statement::BeginTransaction))
}

fn parse_commit(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("COMMIT")(input)?;
    Ok((input, Statement::Commit))
}

fn parse_savepoint(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("SAVEPOINT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = ident(input)?;
    Ok((input, Statement::Savepoint(SavepointStatement { name: name.to_string() })))
}

fn parse_rollback_to_savepoint(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("ROLLBACK TO SAVEPOINT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = ident(input)?;
    Ok((input, Statement::RollbackToSavepoint(name.to_string())))
}

fn parse_release_savepoint(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("RELEASE SAVEPOINT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = ident(input)?;
    Ok((input, Statement::ReleaseSavepoint(name.to_string())))
}

impl Statement {
    pub fn parse(input: &str) -> IResult<&str, Statement> {
        let (input, _) = multispace0(input)?;
        let (input, stmt) = alt((
            CreateStatement::parse,
            InsertStatement::parse,
            SelectStatement::parse,
            UpdateStatement::parse,
            DeleteStatement::parse,
            AlterStatement::parse,
            DropStatement::parse,
            CreateIndexStatement::parse,
            DropIndexStatement::parse,
            parse_savepoint,
            parse_rollback_to_savepoint,
            parse_release_savepoint,
            parse_begin_transaction,
            parse_commit,
        ))(input)?;
        let (input, _) = multispace0(input)?;
        if !input.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof
            )));
        }
        Ok((input, stmt))
    }
}
