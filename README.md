# ToyDB

ToyDB is a minimalistic, in-memory and on-disk database management system written in Rust, implementing basic SQL query capabilities and full-text search.

## Features

- In-Memory or On-Disk storage options
- Basic SQL statements (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE)
- INNER JOIN support
- Full-Text Search using Inverted Index
- Custom data types (INTEGER, TEXT, FTS_TEXT)

## Dependencies

- [nom](https://github.com/Geal/nom) for SQL parsing
- [serde](https://github.com/serde-rs/serde) for serialization
- [bincode](https://github.com/bincode-org/bincode) for encoding

## Usage

To use ToyDB, you can choose between an in-memory storage (`InMemoryToyDB`) or on-disk storage (`OnDiskToyDB`). 

### In-Memory ToyDB Example

```rust
use toydb::InMemoryToyDB;

fn main() {
    let mut db = InMemoryToyDB::new();

    let queries = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "SELECT * FROM users WHERE id = 1",
    ];

    for query in queries {
        let result = db.query(query);
        println!("Result: {:?}", result);
    }
}
```

### On-Disk ToyDB Example

```rust
use toydb::OnDiskToyDB;

fn main() {
    let kv_path = "kv.db";
    let index = "index.bin";
    let mut db = OnDiskToyDB::new(kv_path.to_string(), index.to_string());

    let queries = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "SELECT * FROM users WHERE id = 1",
    ];

    for query in queries {
        let result = db.query(query);
        println!("Result: {:?}", result);
    }
}
```

### Full-Text Search Example

```rust
use toydb::InMemoryToyDB;

fn main() {
    let mut db = InMemoryToyDB::new();

    let queries = vec![
        "CREATE TABLE books (title TEXT, author TEXT, description FTS_TEXT)",
        "INSERT INTO books VALUES ('Book 1', 'Author 1', 'A book about the history of computer science.')",
        "INSERT INTO books VALUES ('Book 2', 'Author 2', 'A book about modern programming languages.')",
        "INSERT INTO books VALUES ('Book 3', 'Author 3', 'A book about the future of artificial intelligence.')",
        "SELECT title, author FROM books WHERE description MATCH 'computer science'",
    ];

    for query in queries {
        let result = db.query(query);
        println!("Result: {:?}", result);
    }
}
```

### DELETE Example

```rust
use toydb::InMemoryToyDB;

fn main() {
    let mut db = InMemoryToyDB::new();

    let queries = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "DELETE FROM users WHERE id = 1",
        "SELECT * FROM users",
    ];

    for query in queries {
        let result = db.query(query);
        println!("Result: {:?}", result);
    }
}
```

### UPDATE Example

```rust
use toydb::InMemoryToyDB;

fn main() {
    let mut db = InMemoryToyDB::new();

    let queries = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "UPDATE users SET name = 'Charlie' WHERE id = 1",
        "SELECT * FROM users",
    ];

    for query in queries {
        let result = db.query(query);
        println!("Result: {:?}", result);
    }
}
```

### INNER JOIN Example

```rust
use toydb::InMemoryToyDB;

fn main() {
    let mut db = InMemoryToyDB::new();

    let queries = vec![
        "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)",
        "INSERT INTO authors VALUES (1, 'Alice')",
        "INSERT INTO authors VALUES (2, 'Bob')",
        "INSERT INTO books VALUES (1, 'Book 1', 1)",
        "INSERT INTO books VALUES (2, 'Book 2', 2)",
        "SELECT authors.name, books.title FROM authors INNER JOIN books ON authors.id = books.author_id",
    ];

    for query in queries {
        let result = db.query(query);
        println!("Result: {:?}", result);
    }
}
```

## Future Improvements (TODOs)

- [ ] Implement support for more SQL statements such as ALTER TABLE, DROP TABLE, and additional JOIN types (LEFT, RIGHT, OUTER).
- [ ] Add support for indexing and query optimization to improve performance.
- [ ] Implement transaction support and atomicity for database operations (there is a transaction struct but it's not in autocommit mode like in sqlite)
- [ ] Add support for user-defined functions, aggregate functions (SUM, COUNT, AVG, MIN, MAX), Grouping and sorting (GROUP BY and ORDER BY), and stored procedures.
- [ ] Improve error handling and reporting.
- [ ] Enhance the full-text search capability with more advanced text processing techniques such as stemming, tokenization, and handling of synonyms.
- [ ] Implement authentication and authorization mechanisms for secure access to the database.
- [ ] Add support for replication and distributed database management (using raft-rs?)
- [ ] Implement a command-line interface or GUI for interacting with the database.
- [ ] Improve documentation and provide examples for using the database in various use cases.
- [ ] Write benchmarks and performance tests to measure and optimize the database performance.
- [ ] Enforce constraints such as unique, primary key, foreign key, and check constraints to maintain data integrity.
- [ ] Implement multi-threading and concurrency control for improved performance and safe parallel access to the database
- [ ] Add support for handling various data types (e.g., date and time, binary data) and user-defined data types.
- [ ] Optimize memory management and caching mechanisms for efficient resource utilization.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more information.
