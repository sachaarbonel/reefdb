mod storage;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0, multispace1},
    combinator::map,
    multi::separated_list0,
    sequence::{delimited, preceded, separated_pair, terminated},
    IResult,
};
use storage::Storage;
use storage::memory::InMemoryStorage;
use std::collections::HashMap;

struct ToyDB<S: Storage> {
    tables: S,
}

impl<S: Storage> ToyDB<S> {
    pub fn new() -> Self {
        ToyDB { tables: S::new() }
    }

    fn execute_statement(&mut self, stmt: Statement) {
        match stmt {
            Statement::Create(CreateTable::Table(table_name, cols)) => {
                self.tables.insert(table_name, Vec::new());
            }
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                if let Some(table) = self.tables.get_mut(&table_name) {
                    table.push(values);
                } else {
                    eprintln!("Table not found: {}", table_name);
                }
            }
            Statement::Select(SelectStatement::FromTable(table_name)) => {
                if let Some(table) = self.tables.get(&table_name) {
                    for row in table {
                        println!("{:?}", row);
                    }
                } else {
                    eprintln!("Table not found: {}", table_name);
                }
            }
        }
    }
}

#[derive(Debug)]
enum CreateTable {
    Table(String, Vec<ColumnDef>),
}

#[derive(Debug)]
struct ColumnDef {
    name: String,
    data_type: DataType,
}

#[derive(Debug)]
enum DataType {
    Text,
    Integer,
}

#[derive(Debug)]
enum InsertStatement {
    IntoTable(String, Vec<String>),
}

#[derive(Debug)]
enum SelectStatement {
    FromTable(String),
}

#[derive(Debug)]
enum Statement {
    Create(CreateTable),
    Insert(InsertStatement),
    Select(SelectStatement),
}

fn parse_create(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag("CREATE TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = alphanumeric1(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns) = delimited(
        tag("("),
        separated_list0(terminated(tag(","), multispace0), parse_column_def),
        tag(")"),
    )(input)?;

    Ok((
        input,
        Statement::Create(CreateTable::Table(table_name.to_string(), columns)),
    ))
}

fn parse_insert(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag("INSERT INTO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = alphanumeric1(input)?;
    let (input, _) = multispace1(input)?;
    let (input, values) = delimited(
        tag("("),
        separated_list0(terminated(tag(","), multispace0), alphanumeric1),
        tag(")"),
    )(input)?;

    let values = values.into_iter().map(|value| value.to_string()).collect();
    Ok((
        input,
        Statement::Insert(InsertStatement::IntoTable(table_name.to_string(), values)),
    ))
}

fn parse_select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag("SELECT * FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = alphanumeric1(input)?;

    Ok((
        input,
        Statement::Select(SelectStatement::FromTable(table_name.to_string())),
    ))
}

fn parse_statement(input: &str) -> IResult<&str, Statement> {
    preceded(multispace0, alt((parse_create, parse_insert, parse_select)))(input)
}



fn parse_data_type(input: &str) -> IResult<&str, DataType> {
    alt((
        map(tag("TEXT"), |_| DataType::Text),
        map(tag("INTEGER"), |_| DataType::Integer),
    ))(input)
}

fn parse_column_def(input: &str) -> IResult<&str, ColumnDef> {
    let (input, name) = alphanumeric1(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_type) = parse_data_type(input)?;

    Ok((
        input,
        ColumnDef {
            name: name.to_string(),
            data_type,
        },
    ))
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_database() {
        let mut db: ToyDB<InMemoryStorage> = ToyDB::new();

        let statements = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users (alice, 30)",
            "INSERT INTO users (bob, 28)",
            "SELECT * FROM users",
        ];

        for statement in statements {
            match parse_statement(statement) {
                Ok((_, stmt)) => {
                    // Execute the parsed statement
                    db.execute_statement(stmt);
                }
                Err(err) => eprintln!("Failed to parse statement: {}", err),
            }
        }

        // Check if the users table has been created
        assert!(db.tables.contains_key(&"users".to_string()));

        // Get the users table and check the number of rows
        let users = db.tables.get(&"users".to_string()).unwrap();
        println!("{:?}", users);
        assert_eq!(users.len(), 2);

        // Check the contents of the users table
        assert_eq!(users[0], vec!["alice", "30"]);
        assert_eq!(users[1], vec!["bob", "28"]);
    }
}
