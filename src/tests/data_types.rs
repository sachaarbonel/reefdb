use crate::sql::data_value::DataValue;
use crate::ReefDBResult;
use crate::InMemoryReefDB;
use crate::ReefDBError;
use chrono::{NaiveDate, NaiveDateTime};

#[test]
fn test_data_types() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create table with all data types
    db.query("CREATE TABLE test_types (int_col INTEGER, text_col TEXT, bool_col BOOLEAN, float_col FLOAT, date_col DATE, timestamp_col TIMESTAMP, tsvector_col TSVECTOR)")?;

    // Insert test data
    db.query("INSERT INTO test_types VALUES (123, 'Hello World', true, 45.67, '2024-03-14', '2024-03-14 12:34:56', 'This is a test document')")?;

    // Query and verify each data type
    if let ReefDBResult::Select(rows) = db.query("SELECT * FROM test_types")? {
        assert_eq!(rows.len(), 1);
        let row = &rows[0].1;
        
        assert_eq!(row[0], DataValue::Integer(123));
        assert_eq!(row[1], DataValue::Text("Hello World".to_string()));
        assert_eq!(row[2], DataValue::Boolean(true));
        assert_eq!(row[3], DataValue::Float(45.67));
        assert_eq!(row[4], DataValue::Date(NaiveDate::from_ymd_opt(2024, 3, 14).unwrap()));
        assert_eq!(row[5], DataValue::Timestamp(NaiveDateTime::parse_from_str("2024-03-14 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap()));
        assert_eq!(row[6], DataValue::Text("This is a test document".to_string()));
    }

    // Test filtering with each data type
    let queries = vec![
        "SELECT * FROM test_types WHERE int_col = 123",
        "SELECT * FROM test_types WHERE text_col = 'Hello World'",
        "SELECT * FROM test_types WHERE bool_col = true",
        "SELECT * FROM test_types WHERE float_col > 45.0",
        "SELECT * FROM test_types WHERE date_col = '2024-03-14'",
        "SELECT * FROM test_types WHERE timestamp_col = '2024-03-14 12:34:56'",
    ];

    for query in queries {
        if let ReefDBResult::Select(rows) = db.query(query)? {
            assert_eq!(rows.len(), 1, "Query failed: {}", query);
        }
    }

    Ok(())
} 