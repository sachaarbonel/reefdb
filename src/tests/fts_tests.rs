use crate::{
    error::ReefDBError,
    result::ReefDBResult,
    InMemoryReefDB,
};

#[test]
fn test_full_text_search_e2e() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create table with a tsvector column
    db.query("CREATE TABLE articles(id INTEGER PRIMARY KEY,title TEXT,content TSVECTOR,language TEXT)")?;

    // Create GIN index for full-text search
    db.query("CREATE GIN INDEX ON articles(content)")?;

    // Insert test articles
    let test_articles = [
        "INSERT INTO articles(id,title,content,language) VALUES(1,'Rust Programming Guide','Learn the Rust programming language from scratch. Understand ownership, borrowing, and lifetimes. Build reliable and efficient software with Rust''s safety guarantees.','english')",
        "INSERT INTO articles(id,title,content,language) VALUES(2,'Web Development with Rust','Building modern web applications using Rust and WebAssembly. Create fast, secure, and scalable web services with Rust frameworks.','english')",
        "INSERT INTO articles(id,title,content,language) VALUES(3,'Database Design Patterns','Understanding database design principles and patterns. Learn about normalization, indexing, and query optimization.','english')",
        "INSERT INTO articles(id,title,content,language) VALUES(4,'Rust and WebAssembly','Compile Rust to WebAssembly for high-performance web applications. Integrate Rust with JavaScript and modern web frameworks.','english')",
    ];

    for query in test_articles {
        db.query(query)?;
    }

    // Test Case 1: Basic single-word search
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')"
    )? {
        assert_eq!(results.len(), 3); // Should match articles 1, 2, and 4
    } else {
        panic!("Expected Select result");
    }

    // Test Case 2: Phrase search
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('web & applications')"
    )? {
        assert_eq!(results.len(), 2); // Should match articles 2 and 4
    } else {
        panic!("Expected Select result");
    }

    // Test Case 3: Boolean operators
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust & web & !database')"
    )? {
        assert_eq!(results.len(), 2); // Should match articles 2 and 4
    } else {
        panic!("Expected Select result");
    }

    // Test Case 4: Complex boolean expression
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust & web | database')"
    )? {
        assert_eq!(results.len(), 3); // Should match articles 1, 2, and 4 (rust & web) or article 3 (database)
    } else {
        panic!("Expected Select result");
    }

    // Test Case 5: Search with language specification
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector('english', content) @@ to_tsquery('english', 'rust & programming')"
    )? {
        assert_eq!(results.len(), 1); // Should match article 1
    } else {
        panic!("Expected Select result");
    }

    // TODO: Future improvements for full-text search
    // Test Case 6: Ranking results
    let query = "SELECT id,title,ts_rank(to_tsvector(content),to_tsquery('rust')) as rank FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')";
    if let ReefDBResult::Select(results) = db.query(query)? {
        assert_eq!(results.len(), 3); // Articles 1, 2, and 4 contain 'rust'
        // Check that the rank values exist
        assert!(results.rows.iter().all(|(_, values)| values.len() == 3));
    }

    // Test Case 7: Prefix matching (not yet implemented)
    // if let ReefDBResult::Select(results) = db.query(
    //     "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('web:*')"
    // )? {
    //     assert_eq!(results.len(), 2); // Should match articles 2 and 4
    // }

    // Test Case 8: Complex boolean expressions with parentheses (not yet implemented)
    // if let ReefDBResult::Select(results) = db.query(
    //     "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('(rust & (web | database)) | (design & pattern)')"
    // )? {
    //     assert_eq!(results.len(), 3); // Should match articles 2, 3, and 4
    // }

    // Test Case 9: Text highlighting (not yet implemented)
    // if let ReefDBResult::Select(results) = db.query(
    //     "SELECT id,title,ts_headline(content, to_tsquery('rust')) FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')"
    // )? {
    //     assert_eq!(results.len(), 3); // Should match articles 1, 2, and 4 with highlighted matches
    // }

    Ok(())
} 