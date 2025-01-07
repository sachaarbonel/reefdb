# ReefDB

![ReefDB logo](https://user-images.githubusercontent.com/18029834/236632891-643c5a0a-8e26-4e88-9bc2-db69125b295f.png)

ReefDB is a minimalistic, in-memory and on-disk database management system written in Rust, implementing basic SQL query capabilities and full-text search.

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

To use ReefDB, you can choose between an in-memory storage (`InMemoryReefDB`) or on-disk storage (`OnDiskReefDB`). 

### In-Memory ReefDB Example

```rust
use reefdb::InMemoryReefDB;

fn main() {
    let mut db = InMemoryReefDB::new();

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

### On-Disk ReefDB Example

```rust
use reefdb::OnDiskReefDB;

fn main() {
    let kv_path = "kv.db";
    let index = "index.bin";
    let mut db = OnDiskReefDB::new(kv_path.to_string(), index.to_string());

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
use reefdb::InMemoryReefDB;

fn main() {
    let mut db = InMemoryReefDB::new();

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
use reefdb::InMemoryReefDB;

fn main() {
    let mut db = InMemoryReefDB::new();

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
use reefdb::InMemoryReefDB;

fn main() {
    let mut db = InMemoryReefDB::new();

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
use reefdb::InMemoryReefDB;

fn main() {
    let mut db = InMemoryReefDB::new();

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

- [ ] Implement support for more SQL statements:
  - [x] ALTER TABLE
  - [x] DROP TABLE
  - [x] JOIN execution logic
  - [ ] JOIN parsing for:
    - [ ] LEFT JOIN
    - [ ] RIGHT JOIN
    - [ ] OUTER JOIN
    - [ ] CROSS JOIN
    - [ ] FULL JOIN

- [] Add support for indexing and query optimization:
  - [x] Implement CREATE INDEX and DROP INDEX SQL statement parsing
  - [x] Add query optimization logic to use indexes when available
  - [x] Implement index persistence for on-disk storage mode
  - [ ] Add Hash indexes for equality comparisons
  - [ ] Add Bitmap indexes for low-cardinality columns
  - [ ] Support multi-column indexes
  - [ ] Implement covering indexes
  - [ ] Add cost-based optimizer
  - [ ] Implement statistics collection and maintenance
  - [ ] Add query plan generation and visualization
  - [ ] Implement join order optimization
  - [ ] Add index usage statistics
  - [ ] Support index hints in queries

- [ ] Implement transaction support:
  - [x] Basic transaction structure
  - [ ] ACID compliance
  - [ ] Implement autocommit mode
  - [ ] Add SAVEPOINT support
  - [ ] Add ROLLBACK TO SAVEPOINT
  - [ ] Support transaction isolation levels
  - [ ] Implement Write-Ahead Logging (WAL)

- [ ] Add support for advanced SQL features:
  - [ ] Aggregate functions (SUM, COUNT, AVG, MIN, MAX)
  - [ ] GROUP BY clauses
  - [ ] HAVING clauses
  - [ ] ORDER BY clauses
  - [ ] LIMIT and OFFSET
  - [ ] Window functions
  - [ ] Subqueries
  - [ ] Common Table Expressions (CTEs)
  - [ ] Views
  - [ ] Stored procedures
  - [ ] User-defined functions

- [ ] Improve error handling and reporting:
  - [ ] Add detailed error messages
  - [ ] Implement error codes
  - [ ] Add stack traces for debugging
  - [ ] Improve error recovery
  - [ ] Add warning system

- [ ] Enhance full-text search:
  - [ ] Implement stemming
  - [ ] Add tokenization options
  - [ ] Support synonyms
  - [ ] Add language-specific processing
  - [ ] Implement relevance scoring
  - [ ] Add phrase searching
  - [ ] Support fuzzy matching

- [ ] Implement security features:
  - [ ] User authentication
  - [ ] Role-based authorization
  - [ ] Row-level security
  - [ ] Column-level security
  - [ ] Audit logging
  - [ ] SSL/TLS support

- [ ] Add distributed features:
  - [ ] Implement replication using raft-rs
  - [ ] Add master-slave configuration
  - [ ] Support sharding
  - [ ] Implement distributed transactions
  - [ ] Add consensus protocol
  - [ ] Support failover

- [ ] Improve user interface:
  - [ ] Command-line interface
  - [ ] Web-based admin interface
  - [ ] Query visualization
  - [ ] Performance monitoring dashboard
  - [ ] Schema visualization

- [ ] Add comprehensive testing:
  - [ ] Unit tests
  - [ ] Integration tests
  - [ ] Performance benchmarks
  - [ ] Stress tests
  - [ ] Compatibility tests
  - [ ] Security tests

- [ ] Implement constraints:
  - [ ] UNIQUE constraints
  - [ ] PRIMARY KEY constraints
  - [ ] FOREIGN KEY constraints with ON DELETE/UPDATE actions
  - [ ] CHECK constraints
  - [ ] NOT NULL constraints
  - [ ] DEFAULT values

- [ ] Add concurrency support:
  - [ ] Implement multi-threading
  - [ ] Add connection pooling
  - [ ] Implement row-level locking
  - [ ] Add deadlock detection
  - [ ] Support MVCC

- [ ] Expand data type support:
  - [ ] DATE and TIME types
  - [ ] DECIMAL/NUMERIC types
  - [ ] BOOLEAN type
  - [ ] BLOB/BINARY types
  - [ ] Array types
  - [ ] JSON type
  - [ ] User-defined types

- [ ] Optimize resource management:
  - [ ] Implement query result cache
  - [ ] Add index page cache
  - [ ] Optimize memory usage
  - [ ] Add buffer pool management
  - [ ] Implement connection pooling

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more information.
