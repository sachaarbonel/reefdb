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

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more information.