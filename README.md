# ReefDB

![ReefDB logo](https://user-images.githubusercontent.com/18029834/236632891-643c5a0a-8e26-4e88-9bc2-db69125b295f.png)

ReefDB is a minimalistic, in-memory and on-disk database management system written in Rust, implementing basic SQL query capabilities and full-text search.

## Usage

To use ReefDB, you can choose between an in-memory storage (`InMemoryReefDB`) or on-disk storage (`OnDiskReefDB`). 

### Basic Example

```rust
use reefdb::InMemoryReefDB;

fn main() {
    let mut db = InMemoryReefDB::new();

    // Basic SQL operations
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.query("INSERT INTO users VALUES (1, 'Alice')");
    db.query("SELECT * FROM users WHERE id = 1");
    
    // Full-text search
    db.query("CREATE TABLE books (title TEXT, description FTS_TEXT)");
    db.query("INSERT INTO books VALUES ('Book 1', 'A book about computer science')");
    db.query("SELECT title FROM books WHERE description MATCH 'computer'");
    
    // Joins
    db.query("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)");
    db.query("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)");
    db.query("SELECT authors.name, books.title FROM authors INNER JOIN books ON authors.id = books.author_id");
}
```

### On-Disk Storage

```rust
use reefdb::OnDiskReefDB;

fn main() {
    let mut db = OnDiskReefDB::new("db.reef".to_string(), "index.bin".to_string());
    // Use the same SQL queries as with InMemoryReefDB
}
```

## Features

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

### Indexing
- ✅ B-Tree index implementation
- ✅ CREATE INDEX and DROP INDEX support
- ✅ Index persistence for on-disk storage
- ✅ Basic query optimization with indexes

## Dependencies

- [nom](https://github.com/Geal/nom) for SQL parsing
- [serde](https://github.com/serde-rs/serde) for serialization
- [bincode](https://github.com/bincode-org/bincode) for encoding

## Future Improvements

### Critical for Production (Highest Priority)

#### Real-time Index Management
- [ ] Real-time index updates with ACID compliance
- [ ] Index consistency verification
- [ ] Atomic index operations
- [ ] Index recovery mechanisms
- [ ] Concurrent index access

#### Query Processing Essentials
- [ ] Basic aggregate functions (COUNT, SUM)
- [ ] ORDER BY implementation
- [ ] LIMIT and OFFSET support
- [ ] LEFT JOIN support
- [ ] Query timeout mechanism

#### Core Performance Features
- [ ] Memory-mapped storage
  - [ ] Page-level operations
  - [ ] Buffer management
  - [ ] Memory-mapped file handling
  - [ ] Crash recovery
- [ ] Basic query optimization
  - [ ] Statistics-based planning
  - [ ] Index usage optimization
  - [ ] Join order optimization
- [ ] Index compression
- [ ] Parallel query execution

#### Monitoring & Diagnostics Essentials
- [ ] Basic query metrics
- [ ] Index usage statistics
- [ ] Transaction monitoring
- [ ] Error logging and tracing

### High Priority

#### Index Improvements
- [ ] Multi-column indexes
- [ ] Hash indexes for equality comparisons
- [ ] Bitmap indexes for low-cardinality columns
- [ ] Cost-based optimizer
- [ ] Query plan visualization
- [ ] Incremental indexing

#### Additional JOIN Support
- [ ] RIGHT JOIN
- [ ] OUTER JOIN
- [ ] CROSS JOIN
- [ ] FULL JOIN
- [ ] NATURAL JOIN
- [ ] SELF JOIN

#### Advanced Query Processing
- [ ] Additional aggregate functions (AVG, MIN, MAX)
- [ ] GROUP BY and HAVING clauses

#### Monitoring & Diagnostics
- [ ] Index statistics
- [ ] Query explanation
- [ ] Performance metrics
- [ ] Index health checks
- [ ] Query profiling

#### Full-text Search Enhancements
- [ ] Advanced Index Types
  - [ ] BM25 scoring with configurable parameters
  - [ ] TF-IDF with normalization options
  - [ ] Custom scoring functions
  - [ ] Position-aware indexing
  - [ ] Field norms support

- [ ] Query Features
  - [ ] Fuzzy matching with configurable distance
  - [ ] Regular expression support
  - [ ] Range queries
  - [ ] Boolean queries with minimum match
  - [ ] Phrase queries with slop
  - [ ] Query rewriting and optimization
  - [ ] Query expansion

#### Vector Search Capabilities
- [ ] Vector Data Types and Operations
  - [ ] VECTOR(dimensions) data type
  - [ ] Vector similarity operators (<->, <=>, <#>)
  - [ ] Configurable distance metrics (L2, Cosine, Dot Product)
  - [ ] Vector normalization options

- [ ] Dimension-Optimized Indexes
  - [ ] KD-Tree for low dimensions (≤ 8)
  - [ ] HNSW for medium dimensions (≤ 100)
  - [ ] Brute Force with SIMD for high dimensions
  - [ ] Index selection based on dimensionality

- [ ] Advanced Vector Search Features
  - [ ] Approximate Nearest Neighbors (ANN)
  - [ ] Hybrid search (combine with text/filters)
  - [ ] Batch vector operations
  - [ ] Vector quantization
  - [ ] Dynamic index rebuilding
  - [ ] Multi-vector queries

- [ ] Vector Search Optimizations
  - [ ] SIMD acceleration
  - [ ] Parallel search
  - [ ] Memory-mapped vectors
  - [ ] Vector compression
  - [ ] Incremental index updates
  - [ ] Cache-friendly layouts

#### Advanced Text Processing
- [ ] Multiple analyzer support
- [ ] Custom token filters
- [ ] Token position tracking
- [ ] SIMD-accelerated processing
- [ ] Phonetic matching
- [ ] Configurable tokenization pipelines

- [ ] CJK (Chinese, Japanese, Korean) Support
  - [ ] Character-based tokenization
  - [ ] N-gram tokenization
  - [ ] Dictionary-based word segmentation
  - [ ] Language-specific stop words
  - [ ] Unicode normalization
  - [ ] Ideograph handling
  - [ ] Reading/pronunciation support
    - [ ] Pinyin for Chinese
    - [ ] Hiragana/Katakana for Japanese
    - [ ] Hangul/Hanja for Korean
  - [ ] Mixed script handling
  - [ ] CJK-specific scoring adjustments
  - [ ] Compound word processing
  - [ ] Character variant normalization

- [ ] Faceted Search
  - [ ] Hierarchical facets
  - [ ] Dynamic facet counting
  - [ ] Custom facet ordering
  - [ ] Multi-value facets

- [ ] Enhanced Scoring & Ranking
  - [ ] Configurable scoring algorithms
  - [ ] Score explanation
  - [ ] Custom boosting factors
  - [ ] Field-weight customization
  - [ ] Position-based scoring

- [ ] Search Quality
  - [ ] Highlighting with snippets
  - [ ] Relevance tuning tools
  - [ ] Search quality metrics

### Medium Priority

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
