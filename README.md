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

## Completed Features

### Core Database Features
- ✅ In-Memory and On-Disk storage modes
- ✅ Basic SQL statements (CREATE, INSERT, SELECT, UPDATE, DELETE)
- ✅ ALTER TABLE with ADD/DROP/RENAME column support
- ✅ DROP TABLE functionality
- ✅ INNER JOIN support
- ✅ Primary key constraints
- ✅ Basic error handling system

### Full-Text Search
- ✅ FTS_TEXT data type
- ✅ Inverted index implementation
- ✅ Basic tokenization
- ✅ Memory and disk-based index storage
- ✅ MATCH operator for text search

### Transaction Support
- ✅ Basic transaction structure
- ✅ Transaction isolation levels (ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)
- ✅ Write-Ahead Logging (WAL)
- ✅ Transaction manager with locking mechanism
- ✅ Full ACID compliance
- ✅ Deadlock detection
- ✅ MVCC implementation

### Transaction Example

```rust
use reefdb::{OnDiskReefDB, transaction::IsolationLevel};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = OnDiskReefDB::create_on_disk("db.reef".to_string(), "index.bin".to_string())?;

    // Begin a transaction with Serializable isolation level
    let tx_id = db.begin_transaction(IsolationLevel::Serializable)?;

    // Execute statements within the transaction
    let queries = vec![
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
        "INSERT INTO accounts VALUES (1, 1000)",
        "INSERT INTO accounts VALUES (2, 500)",
        "UPDATE accounts SET balance = balance - 100 WHERE id = 1",
        "UPDATE accounts SET balance = balance + 100 WHERE id = 2",
    ];

    for query in queries {
        match db.query(query) {
            Ok(_) => continue,
            Err(e) => {
                // Rollback on error
                db.rollback_transaction(tx_id)?;
                return Err(Box::new(e));
            }
        }
    }

    // Commit the transaction
    db.commit_transaction(tx_id)?;
    Ok(())
}
```

### MVCC Example

```rust
use reefdb::{OnDiskReefDB, transaction::IsolationLevel};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = OnDiskReefDB::create_on_disk("db.reef".to_string(), "index.bin".to_string())?;

    // Start two concurrent transactions
    let tx1_id = db.begin_transaction(IsolationLevel::Serializable)?;
    let tx2_id = db.begin_transaction(IsolationLevel::Serializable)?;

    // Each transaction sees its own version of the data
    db.query("CREATE TABLE test (id INTEGER, value TEXT)")?;
    db.query("INSERT INTO test VALUES (1, 'initial')")?;

    // Transaction 1 updates the value
    db.query("UPDATE test SET value = 'tx1_update' WHERE id = 1")?;

    // Transaction 2 still sees the original value
    let result = db.query("SELECT value FROM test WHERE id = 1")?;
    assert_eq!(result.to_string(), "initial");

    // Commit both transactions
    db.commit_transaction(tx1_id)?;
    db.commit_transaction(tx2_id)?;
    Ok(())
}
```

### Indexing
- ✅ B-Tree index implementation
- ✅ CREATE INDEX and DROP INDEX support
- ✅ Index persistence for on-disk storage
- ✅ Basic query optimization with indexes

## Future Improvements (TODOs)

### High Priority

#### Query Processing
- [ ] Additional JOIN types:
  - [ ] LEFT JOIN
  - [ ] RIGHT JOIN
  - [ ] OUTER JOIN
  - [ ] CROSS JOIN
  - [ ] FULL JOIN
- [ ] Aggregate functions (SUM, COUNT, AVG, MIN, MAX)
- [ ] GROUP BY and HAVING clauses
- [ ] ORDER BY clauses
- [ ] LIMIT and OFFSET support

#### Transaction Enhancements
- [x] Full ACID compliance
- [ ] Autocommit mode
- [ ] SAVEPOINT support
- [x] Deadlock detection
- [x] MVCC implementation

#### Index Improvements
- [ ] Multi-column indexes
- [ ] Hash indexes for equality comparisons
- [ ] Bitmap indexes for low-cardinality columns
- [ ] Cost-based optimizer
- [ ] Query plan visualization

### Medium Priority

#### Full-text Search Enhancements
- [ ] Stemming support
- [ ] Advanced tokenization options
- [ ] Relevance scoring
- [ ] Phrase searching
- [ ] Fuzzy matching
- [ ] Synonym support

#### Constraint System
- [ ] UNIQUE constraints
- [ ] CHECK constraints
- [ ] NOT NULL constraints
- [ ] DEFAULT values
- [ ] Enhanced FOREIGN KEY support with ON DELETE/UPDATE actions

#### Query Features
- [ ] Subqueries
- [ ] Common Table Expressions (CTEs)
- [ ] Views
- [ ] Stored procedures
- [ ] User-defined functions

### Lower Priority

#### Data Types
- [ ] DATE and TIME types
- [ ] DECIMAL/NUMERIC types
- [ ] BOOLEAN type
- [ ] BLOB/BINARY types
- [ ] Array types
- [ ] JSON type
- [ ] User-defined types

#### Security Features
- [ ] User authentication
- [ ] Role-based authorization
- [ ] Row-level security
- [ ] Column-level security
- [ ] Audit logging
- [ ] SSL/TLS support

#### Distributed Features
- [ ] Replication using raft-rs
- [ ] Master-slave configuration
- [ ] Sharding support
- [ ] Distributed transactions
- [ ] Failover support

#### Performance Optimization
- [ ] Query result cache
- [ ] Index page cache
- [ ] Buffer pool management
- [ ] Connection pooling
- [ ] Query plan optimization

#### Developer Experience
- [ ] Command-line interface
- [ ] Web-based admin interface
- [ ] Query visualization
- [ ] Performance monitoring dashboard
- [ ] Schema visualization
- [ ] Comprehensive documentation

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more information.

### ACID Compliance

ReefDB now provides full ACID (Atomicity, Consistency, Isolation, Durability) compliance:

- **Atomicity**: Transactions are all-or-nothing. If any part fails, the entire transaction is rolled back.
- **Consistency**: The database moves from one valid state to another, maintaining all constraints.
- **Isolation**: Concurrent transactions don't interfere with each other, using MVCC and proper locking.
- **Durability**: Committed transactions are persisted to disk using Write-Ahead Logging.

### Deadlock Detection

ReefDB implements deadlock detection using a wait-for graph algorithm:

- Automatically detects circular wait conditions between transactions
- Selects appropriate victim transactions to break deadlocks
- Provides graceful recovery by rolling back affected transactions
- Integrates with the transaction manager for seamless handling

### Deadlock Example

```rust
use reefdb::{OnDiskReefDB, transaction::IsolationLevel};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = OnDiskReefDB::create_on_disk("db.reef".to_string(), "index.bin".to_string())?;

    // Start two concurrent transactions
    let tx1_id = db.begin_transaction(IsolationLevel::Serializable)?;
    let tx2_id = db.begin_transaction(IsolationLevel::Serializable)?;

    // Transaction 1 updates table1
    db.query("UPDATE table1 SET value = 'new' WHERE id = 1")?;

    // Transaction 2 updates table2
    db.query("UPDATE table2 SET value = 'new' WHERE id = 1")?;

    // Potential deadlock: T1 tries to access T2's resource and vice versa
    match db.query("UPDATE table2 SET value = 'new2' WHERE id = 2") {
        Ok(_) => (),
        Err(e) => {
            // Handle deadlock error
            if e.to_string().contains("deadlock") {
                db.rollback_transaction(tx1_id)?;
            }
        }
    }

    // Clean up
    db.commit_transaction(tx2_id)?;
    Ok(())
}
```
