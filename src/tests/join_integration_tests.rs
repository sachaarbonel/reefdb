#[cfg(test)]
mod tests {
    use std::fs;
    use crate::{
        error::ReefDBError,
        result::ReefDBResult,
        sql::{
            statements::Statement,
            data_value::DataValue,
            data_type::DataType,
        },
        transaction::IsolationLevel,
        InMemoryReefDB,
    };

    type Result<T> = std::result::Result<T, ReefDBError>;

    fn cleanup_test_files(kv_path: &str, index_path: &str) {
        let _ = fs::remove_file(kv_path);
        let _ = fs::remove_file(index_path);
    }

    #[test]
    fn test_basic_join() -> Result<()> {
        let kv_path = "join_test_kv.db";
        let index_path = "join_test_index.bin";

        cleanup_test_files(kv_path, index_path);

        let mut db = InMemoryReefDB::create_in_memory()?;

        // Begin a transaction for setup
        let setup_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create authors table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)").unwrap().1)?;

        // Create books table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)").unwrap().1)?;

        // Insert test data
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (1, 'Alice')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (2, 'Bob')").unwrap().1)?;

        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (1, 'Book 1', 1)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (2, 'Book 2', 2)").unwrap().1)?;

        // Commit setup
        db.transaction_manager.as_mut().unwrap().commit_transaction(setup_tx)?;

        // Begin a transaction for the join query
        let query_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        let select_stmt = Statement::parse(
            "SELECT authors.name, books.title FROM authors INNER JOIN books ON authors.id = books.author_id"
        ).unwrap().1;

        let result = db.transaction_manager.as_mut().unwrap().execute_statement(query_tx, select_stmt)?;

        if let ReefDBResult::Select(results) = result {
            assert_eq!(results.len(), 2, "Expected 2 rows from join");
            
            // Verify column information
            assert_eq!(results.columns.len(), 2);
            assert_eq!(results.columns[0].name, "name");
            assert_eq!(results.columns[0].data_type, DataType::Text);
            assert_eq!(results.columns[0].table, Some("authors".to_string()));
            assert_eq!(results.columns[1].name, "title");
            assert_eq!(results.columns[1].data_type, DataType::Text);
            assert_eq!(results.columns[1].table, Some("books".to_string()));
            
            // Verify all combinations exist
            let expected_combinations = vec![
                (DataValue::Text("Alice".to_string()), DataValue::Text("Book 1".to_string())),
                (DataValue::Text("Bob".to_string()), DataValue::Text("Book 2".to_string())),
            ];

            for (name, title) in expected_combinations {
                assert!(results.rows.iter().any(|(_, row)| {
                    row[0] == name && row[1] == title
                }), "Missing combination: {:?} - {:?}", name, title);
            }
        } else {
            panic!("Expected Select result");
        }

        cleanup_test_files(kv_path, index_path);
        Ok(())
    }

    #[test]
    fn test_inner_join_with_where_clause() -> Result<()> {
        let kv_path = "join_where_test_kv.db";
        let index_path = "join_where_test_index.bin";

        cleanup_test_files(kv_path, index_path);

        let mut db = InMemoryReefDB::create_in_memory()?;

        // Begin a transaction for setup
        let setup_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create authors table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)").unwrap().1)?;

        // Create books table with year column
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, year INTEGER)").unwrap().1)?;

        // Insert test data
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (1, 'Alice')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (2, 'Bob')").unwrap().1)?;

        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (1, 'Book 1', 1, 2020)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (2, 'Book 2', 2, 2021)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (3, 'Book 3', 1, 2022)").unwrap().1)?;

        // Commit setup
        db.transaction_manager.as_mut().unwrap().commit_transaction(setup_tx)?;

        // Begin a transaction for the join query
        let query_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        let select_stmt = Statement::parse(
            "SELECT authors.name, books.title, books.year 
             FROM authors 
             INNER JOIN books ON authors.id = books.author_id 
             WHERE books.year > 2020"
        ).unwrap().1;

        let result = db.transaction_manager.as_mut().unwrap().execute_statement(query_tx, select_stmt)?;

        if let ReefDBResult::Select(results) = result {
            assert_eq!(results.len(), 2, "Expected 2 rows matching year > 2020");
            
            // Verify column information
            assert_eq!(results.columns.len(), 3);
            assert_eq!(results.columns[0].name, "name");
            assert_eq!(results.columns[0].data_type, DataType::Text);
            assert_eq!(results.columns[0].table, Some("authors".to_string()));
            assert_eq!(results.columns[1].name, "title");
            assert_eq!(results.columns[1].data_type, DataType::Text);
            assert_eq!(results.columns[1].table, Some("books".to_string()));
            assert_eq!(results.columns[2].name, "year");
            assert_eq!(results.columns[2].data_type, DataType::Integer);
            assert_eq!(results.columns[2].table, Some("books".to_string()));
            
            // Verify specific combinations
            let expected_combinations = vec![
                ("Bob", "Book 2", 2021),
                ("Alice", "Book 3", 2022),
            ];

            for (name, title, year) in expected_combinations {
                assert!(results.rows.iter().any(|(_, row)| {
                    row[0] == DataValue::Text(name.to_string()) &&
                    row[1] == DataValue::Text(title.to_string()) &&
                    row[2] == DataValue::Integer(year)
                }), "Missing combination: {} - {} - {}", name, title, year);
            }
        } else {
            panic!("Expected Select result");
        }

        cleanup_test_files(kv_path, index_path);
        Ok(())
    }

    #[test]
    fn test_inner_join_multiple_conditions() -> Result<()> {
        let kv_path = "join_multi_test_kv.db";
        let index_path = "join_multi_test_index.bin";

        cleanup_test_files(kv_path, index_path);

        let mut db = InMemoryReefDB::create_in_memory()?;

        // Begin a transaction for setup
        let setup_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create authors table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT, country TEXT)").unwrap().1)?;

        // Create books table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, genre TEXT)").unwrap().1)?;

        // Insert test data for authors
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (1, 'Alice', 'USA')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (2, 'Bob', 'UK')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (3, 'Charlie', 'USA')").unwrap().1)?;

        // Insert test data for books
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (1, 'Mystery 1', 1, 'Mystery')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (2, 'Romance 1', 2, 'Romance')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (3, 'Mystery 2', 3, 'Mystery')").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (4, 'Mystery 3', 1, 'Mystery')").unwrap().1)?;

        // Commit setup
        db.transaction_manager.as_mut().unwrap().commit_transaction(setup_tx)?;

        // Begin a transaction for the join query
        let query_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        let select_stmt = Statement::parse(
            "SELECT authors.name, books.title 
             FROM authors 
             INNER JOIN books ON authors.id = books.author_id 
             WHERE authors.country = 'USA' AND books.genre = 'Mystery'"
        ).unwrap().1;

        let result = db.transaction_manager.as_mut().unwrap().execute_statement(query_tx, select_stmt)?;

        if let ReefDBResult::Select(results) = result {
            assert_eq!(results.len(), 3, "Expected 3 mystery books by USA authors");
            
            // Verify column information
            assert_eq!(results.columns.len(), 2);
            assert_eq!(results.columns[0].name, "name");
            assert_eq!(results.columns[0].data_type, DataType::Text);
            assert_eq!(results.columns[0].table, Some("authors".to_string()));
            assert_eq!(results.columns[1].name, "title");
            assert_eq!(results.columns[1].data_type, DataType::Text);
            assert_eq!(results.columns[1].table, Some("books".to_string()));
            
            // Verify specific combinations
            let expected_combinations = vec![
                ("Alice", "Mystery 1"),
                ("Alice", "Mystery 3"),
                ("Charlie", "Mystery 2"),
            ];

            for (name, title) in expected_combinations {
                assert!(results.rows.iter().any(|(_, row)| {
                    row[0] == DataValue::Text(name.to_string()) &&
                    row[1] == DataValue::Text(title.to_string())
                }), "Missing combination: {} - {}", name, title);
            }
        } else {
            panic!("Expected Select result");
        }

        cleanup_test_files(kv_path, index_path);
        Ok(())
    }

    #[test]
    fn test_inner_join_complex_conditions() -> Result<()> {
        let kv_path = "join_complex_test_kv.db";
        let index_path = "join_complex_test_index.bin";

        cleanup_test_files(kv_path, index_path);

        let mut db = InMemoryReefDB::create_in_memory()?;

        // Begin a transaction for setup
        let setup_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create authors table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT, country TEXT, age INTEGER)").unwrap().1)?;

        // Create books table
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, genre TEXT, year INTEGER)").unwrap().1)?;

        // Insert test data for authors
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (1, 'Alice', 'USA', 30)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (2, 'Bob', 'UK', 25)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (3, 'Charlie', 'USA', 35)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO authors VALUES (4, 'David', 'USA', 40)").unwrap().1)?;

        // Insert test data for books
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (1, 'Mystery 1', 1, 'Mystery', 2020)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (2, 'Romance 1', 2, 'Romance', 2021)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (3, 'Mystery 2', 3, 'Mystery', 2022)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (4, 'Mystery 3', 1, 'Mystery', 2023)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx,
            Statement::parse("INSERT INTO books VALUES (5, 'Romance 2', 4, 'Romance', 2023)").unwrap().1)?;

        // Commit setup
        db.transaction_manager.as_mut().unwrap().commit_transaction(setup_tx)?;

        // Begin a transaction for the join query
        let query_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Test complex AND condition
        let select_stmt = Statement::parse(
            "SELECT authors.name, books.title 
             FROM authors 
             INNER JOIN books ON authors.id = books.author_id 
             WHERE authors.country = 'USA' AND books.genre = 'Mystery' AND books.year > 2021"
        ).unwrap().1;

        let result = db.transaction_manager.as_mut().unwrap().execute_statement(query_tx, select_stmt)?;

        if let ReefDBResult::Select(results) = result {
            assert_eq!(results.len(), 2, "Expected 2 mystery books by USA authors after 2021");
            
            // Verify column information
            assert_eq!(results.columns.len(), 2);
            assert_eq!(results.columns[0].name, "name");
            assert_eq!(results.columns[0].data_type, DataType::Text);
            assert_eq!(results.columns[0].table, Some("authors".to_string()));
            assert_eq!(results.columns[1].name, "title");
            assert_eq!(results.columns[1].data_type, DataType::Text);
            assert_eq!(results.columns[1].table, Some("books".to_string()));
            
            // Verify specific combinations
            let expected_combinations = vec![
                ("Charlie", "Mystery 2"),
                ("Alice", "Mystery 3"),
            ];

            for (name, title) in expected_combinations {
                assert!(results.rows.iter().any(|(_, row)| {
                    row[0] == DataValue::Text(name.to_string()) &&
                    row[1] == DataValue::Text(title.to_string())
                }), "Missing combination: {} - {}", name, title);
            }
        } else {
            panic!("Expected Select result");
        }

        // Test OR condition
        let select_stmt = Statement::parse(
            "SELECT authors.name, books.title 
             FROM authors 
             INNER JOIN books ON authors.id = books.author_id 
             WHERE (authors.age > 35 AND books.genre = 'Romance') OR (authors.country = 'UK' AND books.year = 2021)"
        ).unwrap().1;

        let result = db.transaction_manager.as_mut().unwrap().execute_statement(query_tx, select_stmt)?;

        if let ReefDBResult::Select(results) = result {
            assert_eq!(results.len(), 2, "Expected 2 rows matching complex OR condition");
            
            // Verify column information
            assert_eq!(results.columns.len(), 2);
            assert_eq!(results.columns[0].name, "name");
            assert_eq!(results.columns[0].data_type, DataType::Text);
            assert_eq!(results.columns[0].table, Some("authors".to_string()));
            assert_eq!(results.columns[1].name, "title");
            assert_eq!(results.columns[1].data_type, DataType::Text);
            assert_eq!(results.columns[1].table, Some("books".to_string()));
            
            // Verify specific combinations
            let expected_combinations = vec![
                ("David", "Romance 2"),  // age > 35 AND genre = 'Romance'
                ("Bob", "Romance 1"),    // country = 'UK' AND year = 2021
            ];

            for (name, title) in expected_combinations {
                assert!(results.rows.iter().any(|(_, row)| {
                    row[0] == DataValue::Text(name.to_string()) &&
                    row[1] == DataValue::Text(title.to_string())
                }), "Missing combination: {} - {}", name, title);
            }
        } else {
            panic!("Expected Select result");
        }

        cleanup_test_files(kv_path, index_path);
        Ok(())
    }
} 