use crate::{
    error::ReefDBError,
    result::ReefDBResult,
    InMemoryReefDB,
    sql::{
        clauses::{
            wheres::where_type::WhereType,
            full_text_search::{FTSClause, TSQuery, QueryType, Language},
        },
        column::Column,
        column_def::ColumnDef,
        data_type::DataType,
        data_value::DataValue,
        statements::{create::CreateStatement, insert::InsertStatement, select::SelectStatement, Statement},
        constraints::constraint::Constraint,
    },
};

#[test]
fn test_fts_search_with_select() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;
    
    // Create table with FTS column
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
        ColumnDef::new("title", DataType::Text, vec![]),
        ColumnDef::new("author", DataType::Text, vec![]),
        ColumnDef::new("description", DataType::TSVector, vec![]),
    ];
    db.execute_statement(Statement::Create(CreateStatement::Table("books".to_string(), columns)))?;

    // Insert test data
    let values = vec![
        vec![
            DataValue::Integer(1),
            DataValue::Text("Book 1".to_string()),
            DataValue::Text("Author 1".to_string()),
            DataValue::Text("A book about the history of computer science.".to_string()),
        ],
        vec![
            DataValue::Integer(2),
            DataValue::Text("Book 2".to_string()),
            DataValue::Text("Author 2".to_string()),
            DataValue::Text("A book about modern programming languages.".to_string()),
        ],
        vec![
            DataValue::Integer(3),
            DataValue::Text("Book 3".to_string()),
            DataValue::Text("Author 3".to_string()),
            DataValue::Text("A book about the future of artificial intelligence.".to_string()),
        ],
    ];

    for value in values {
        db.execute_statement(Statement::Insert(InsertStatement::IntoTable("books".to_string(), value)))?;
    }

    // Test FTS with new syntax
    let column = Column { name: "description".to_string(), table: None };
    let query = TSQuery::new("computer & science".to_string())
        .with_type(QueryType::Plain)
        .with_language(Language::English);

    let where_clause = WhereType::FTS(FTSClause::new(column, query.text)
        .with_language(Language::English)
        .with_query_type(QueryType::Plain));

    let select_stmt = SelectStatement::FromTable(
        "books".to_string(),
        vec![
            Column { name: "id".to_string(), table: None },
            Column { name: "title".to_string(), table: None },
            Column { name: "author".to_string(), table: None },
        ],
        Some(where_clause),
        vec![],
    );

    let result = db.execute_statement(Statement::Select(select_stmt))?;
    
    if let ReefDBResult::Select(rows) = result {
        assert_eq!(rows.len(), 1); // Should find one matching book
        if let DataValue::Integer(id) = &rows[0].1[0] {
            assert_eq!(*id, 1);
        } else {
            panic!("Expected integer ID");
        }
    } else {
        panic!("Expected Select result");
    }

    Ok(())
} 