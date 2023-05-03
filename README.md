
# Toy SQL Database

This project is a minimalistic, in-memory SQL-like database written in Rust. It uses the `nom` library for parsing SQL queries and supports simple SELECT, INSERT, and CREATE TABLE statements. This project serves as an educational example and is not intended for production use.

## Features

- In-memory storage using a HashMap
- Support for CREATE TABLE, INSERT, UPDATE and SELECT statements
- Nom-based parser for SQL-like syntax

## Usage

To use the Toy SQL Database in your Rust project, include the `main.rs` or `lib.rs` file in your `src` folder and import the `ToyDB` struct and parsing functions.

Example usage:

```rust
use toy_sql_database::{ToyDB, parse_statement};

fn main() {
    let mut db = ToyDB::new();

    let statements = vec![
        "CREATE TABLE users (name TEXT, age INTEGER)",
        "INSERT INTO users ('alice', 30)",
        "INSERT INTO users ('bob', 28)",
        "SELECT name, age FROM users",
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
}
```

Please note that this implementation is minimal and limited, lacking many features of a full-fledged SQL database. It is meant as a starting point for learning and experimentation.

## License

This project is available under the MIT License. See the [LICENSE](LICENSE) file for more information.
