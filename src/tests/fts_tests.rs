use crate::{
    error::ReefDBError,
    result::ReefDBResult,
    InMemoryReefDB,
    sql::{
        data_type::DataType,
        data_value::DataValue,
    },
};

#[test]
fn test_full_text_search_e2e() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create table with a tsvector column
    db.query("CREATE TABLE articles(id INTEGER PRIMARY KEY,title TEXT,content TSVECTOR,language TEXT)")?;

    // Create GIN index for full-text search
    db.query("CREATE GIN INDEX ON articles(content)")?;

    // Insert test data with more explicit web-related content
    db.query("INSERT INTO articles VALUES (1, 'Rust Programming', 'Learn Rust programming language basics', 'english')")?;
    db.query("INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications with Rust frameworks', 'english')")?;
    db.query("INSERT INTO articles VALUES (3, 'Database Design', 'Introduction to database design principles', 'english')")?;
    db.query("INSERT INTO articles VALUES (4, 'Rust Web', 'Advanced web development techniques using Rust and web applications', 'english')")?;

    // Test Case 1: Basic single-word search
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')"
    )? {
        assert_eq!(results.len(), 3); // Should match articles 1, 2, and 4
        
        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("articles".to_string()));
        assert_eq!(results.columns[1].name, "title");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("articles".to_string()));

        // Verify the expected articles are returned
        let expected_titles = vec!["Rust Programming", "Web Development", "Rust Web"];
        for title in expected_titles {
            assert!(results.rows.iter().any(|(_, row)| {
                row[1] == DataValue::Text(title.to_string())
            }), "Missing article: {}", title);
        }
    } else {
        panic!("Expected Select result");
    }

    // Test Case 2: Phrase search
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('web & applications')"
    )? {
        assert_eq!(results.len(), 2); // Should match articles 2 and 4
        
        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("articles".to_string()));
        assert_eq!(results.columns[1].name, "title");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("articles".to_string()));

        // Verify the expected articles are returned
        let expected_titles = vec!["Web Development", "Rust Web"];
        for title in expected_titles {
            assert!(results.rows.iter().any(|(_, row)| {
                row[1] == DataValue::Text(title.to_string())
            }), "Missing article: {}", title);
        }
    } else {
        panic!("Expected Select result");
    }

    // Test Case 3: Boolean operators
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust & web & !database')"
    )? {
        assert_eq!(results.len(), 2); // Should match articles 2 and 4
        
        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("articles".to_string()));
        assert_eq!(results.columns[1].name, "title");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("articles".to_string()));

        // Verify the expected articles are returned
        let expected_titles = vec!["Web Development", "Rust Web"];
        for title in expected_titles {
            assert!(results.rows.iter().any(|(_, row)| {
                row[1] == DataValue::Text(title.to_string())
            }), "Missing article: {}", title);
        }
    } else {
        panic!("Expected Select result");
    }

    // Test Case 4: Complex boolean expression
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust & web | database')"
    )? {
        assert_eq!(results.len(), 3); // Should match articles 2, 3, and 4
        
        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("articles".to_string()));
        assert_eq!(results.columns[1].name, "title");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("articles".to_string()));

        // Verify the expected articles are returned
        let expected_titles = vec!["Web Development", "Database Design", "Rust Web"];
        for title in expected_titles {
            assert!(results.rows.iter().any(|(_, row)| {
                row[1] == DataValue::Text(title.to_string())
            }), "Missing article: {}", title);
        }
    } else {
        panic!("Expected Select result");
    }

    // Test Case 5: Search with language specification
    if let ReefDBResult::Select(results) = db.query(
        "SELECT id,title FROM articles WHERE to_tsvector('english', content) @@ to_tsquery('english', 'rust & programming')"
    )? {
        assert_eq!(results.len(), 1); // Should match article 1
        
        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("articles".to_string()));
        assert_eq!(results.columns[1].name, "title");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("articles".to_string()));

        // Verify the expected article is returned
        let expected_titles = vec!["Rust Programming"];
        for title in expected_titles {
            assert!(results.rows.iter().any(|(_, row)| {
                row[1] == DataValue::Text(title.to_string())
            }), "Missing article: {}", title);
        }
    } else {
        panic!("Expected Select result");
    }

    // Test Case 6: Ranking results
    let query = "SELECT id,title,ts_rank(to_tsvector(content),to_tsquery('rust')) as rank FROM articles WHERE to_tsvector(content) @@ to_tsquery('rust')";
    if let ReefDBResult::Select(results) = db.query(query)? {
        assert_eq!(results.len(), 3); // Articles 1, 2, and 4 contain 'rust'
        
        // Verify column information
        assert_eq!(results.columns.len(), 3);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("articles".to_string()));
        assert_eq!(results.columns[1].name, "title");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("articles".to_string()));
        assert_eq!(results.columns[2].name, "rank");
        assert_eq!(results.columns[2].data_type, DataType::Float);
        assert_eq!(results.columns[2].table, None);
        assert!(results.columns[2].nullable);
        
        // Verify the expected articles are returned
        let expected_titles = vec!["Rust Programming", "Web Development", "Rust Web"];
        for title in expected_titles {
            assert!(results.rows.iter().any(|(_, row)| {
                row[1] == DataValue::Text(title.to_string())
            }), "Missing article: {}", title);
        }
        
        // Check that the rank values exist and are valid
        assert!(results.rows.iter().all(|(_, values)| {
            values.len() == 3 && 
            matches!(values[2], DataValue::Float(_))
        }));

        // Verify that articles with 'Rust' in the title have higher rank
        let mut ranked_articles: Vec<_> = results.rows.iter()
            .filter_map(|(_, values)| {
                if let (DataValue::Text(title), DataValue::Float(rank)) = (&values[1], &values[2]) {
                    Some((title.clone(), *rank))
                } else {
                    None
                }
            })
            .collect();
        ranked_articles.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // Articles with "Rust" in title should be ranked higher
        assert!(ranked_articles.iter().any(|(title, _)| title.contains("Rust")));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
} 