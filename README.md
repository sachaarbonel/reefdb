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

    // Create a table with various data types
    db.query("CREATE TABLE records (
        id INTEGER PRIMARY KEY,
        name TEXT,
        active BOOLEAN,
        score FLOAT,
        birth_date DATE,
        last_login TIMESTAMP,
        description TSVECTOR
    )");

    // Insert data with different types
    db.query("INSERT INTO records VALUES (
        1,
        'Alice',
        TRUE,
        95.5,
        '2000-01-01',
        '2024-03-14 12:34:56',
        'Software engineer with expertise in databases'
    )");

    // Query with type-specific operations
    db.query("SELECT * FROM records WHERE score > 90.0");
    db.query("SELECT * FROM records WHERE birth_date > '1999-12-31'");
    db.query("SELECT * FROM records WHERE active = TRUE");
    db.query("SELECT * FROM records WHERE to_tsvector(description) @@ to_tsquery('database')");
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
- ✅ Rich data type support (INTEGER, TEXT, BOOLEAN, FLOAT, DATE, TIMESTAMP, NULL)

### Data Types
- ✅ INTEGER: Whole number values
- ✅ TEXT: String values with support for escaped quotes
- ✅ BOOLEAN: TRUE/FALSE values
- ✅ FLOAT: Decimal number values
- ✅ DATE: Date values in 'YYYY-MM-DD' format
- ✅ TIMESTAMP: Datetime values in 'YYYY-MM-DD HH:MM:SS' format
- ✅ NULL: Null values
- ✅ TSVECTOR: Full-text search optimized text type

### Full-Text Search
- ✅ TSVECTOR data type
- ✅ Inverted index implementation
- ✅ Basic tokenization
- ✅ Memory and disk-based index storage
- ✅ @@ operator for text search

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

#### Query Analysis & Optimization
- [ ] Query Analyzer Framework
  - [ ] Cost-based query planning
  - [ ] Statistics collection and management
  - [ ] Index usage analysis
  - [ ] Join order optimization
  - [ ] Query rewriting
- [ ] Query Plan Visualization
  - [ ] Visual execution plan representation
  - [ ] Cost breakdown analysis
  - [ ] Performance bottleneck identification
- [ ] Statistics Management
  - [ ] Table statistics (row counts, size)
  - [ ] Column statistics (cardinality, distribution)
  - [ ] Index statistics (size, depth, usage)
  - [ ] Automatic statistics updates

#### Query Processing Essentials
- [ ] Basic aggregate functions (COUNT, SUM)
- [ ] ORDER BY implementation
- [ ] LIMIT and OFFSET support
- [ ] LEFT JOIN support
- [ ] Query timeout mechanism

#### Core Performance Features
- [x] Memory-mapped storage
  - [x] Memory-mapped file handling
  - [x] Basic persistence
  - [x] Concurrent access support
  - [ ] Page-level operations
  - [ ] Buffer management
  - [ ] Crash recovery
  - [ ] Dynamic file resizing
  - [ ] Memory-mapped index support
- [ ] Index compression
- [ ] Parallel query execution

#### Monitoring & Diagnostics Essentials
- [ ] Query Performance Metrics
  - [ ] Execution time tracking
  - [ ] Resource usage monitoring
  - [ ] Query plan effectiveness
  - [ ] Index usage statistics
- [ ] Transaction monitoring
- [ ] Error logging and tracing

### High Priority

#### Index Improvements
- [ ] Multi-column indexes
- [ ] Hash indexes for equality comparisons
- [ ] Bitmap indexes for low-cardinality columns
- [ ] Incremental indexing
- [ ] Index maintenance optimization
  - [ ] Background index rebuilding
  - [ ] Index fragmentation analysis
  - [ ] Automatic index suggestions

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
- [ ] Window functions
- [ ] Common Table Expressions (CTEs)
- [ ] Subquery optimization

#### Full-text Search Enhancements
- [ ] Advanced Index Types
  - [x] BM25 scoring with configurable parameters
  - [x] TF-IDF with normalization options
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
  - [ ] Prefix matching (e.g., `web:*`)
  - [ ] Complex boolean expressions with parentheses
  - [ ] Result ranking with `ts_rank`
  - [ ] Text highlighting with `ts_headline`

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

### Medium Priority

#### Query Plan Management
- [ ] Plan caching
- [ ] Adaptive query execution
- [ ] Runtime statistics collection
- [ ] Dynamic plan adjustment
- [ ] Materialized view suggestions

#### Constraint System
- [ ] UNIQUE constraints
- [ ] CHECK constraints
- [ ] NOT NULL constraints
- [ ] DEFAULT values
- [ ] Enhanced FOREIGN KEY support with ON DELETE/UPDATE actions

#### Advanced Features
- [ ] Views
- [ ] Stored procedures
- [ ] User-defined functions
- [ ] Triggers
- [ ] Materialized views

#### CJK (Chinese, Japanese, Korean) Support
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

#### Developer Experience
- [ ] Command-line interface
- [ ] Web-based admin interface
- [ ] Query visualization
- [ ] Performance monitoring dashboard
- [ ] Schema visualization
- [ ] Comprehensive documentation

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more information.
