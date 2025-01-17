use crate::sql::{
    data_type::DataType,
    data_value::DataValue,
    column_def::ColumnDef,
    table::Table,
    constraints::constraint::Constraint,
};

#[test]
fn test_data_types() {
    let mut table = Table::new(vec![
        ColumnDef::new("text_col", DataType::Text, vec![]),
        ColumnDef::new("int_col", DataType::Integer, vec![]),
        ColumnDef::new("bool_col", DataType::Boolean, vec![]),
        ColumnDef::new("float_col", DataType::Float, vec![]),
        ColumnDef::new("date_col", DataType::Date, vec![]),
        ColumnDef::new("timestamp_col", DataType::Timestamp, vec![]),
    ]);

    // Insert a test row
    let row = vec![
        DataValue::Text("Hello".to_string()),
        DataValue::Integer(42),
        DataValue::Boolean(true),
        DataValue::Float(3.14),
        DataValue::Date("2024-03-14".to_string()),
        DataValue::Timestamp("2024-03-14 12:34:56".to_string()),
    ];
    table.insert_row(row);

    // Test type matching
    let schema = table.get_schema();
    assert!(schema[0].data_type == DataType::Text);
    assert!(schema[1].data_type == DataType::Integer);
    assert!(schema[2].data_type == DataType::Boolean);
    assert!(schema[3].data_type == DataType::Float);
    assert!(schema[4].data_type == DataType::Date);
    assert!(schema[5].data_type == DataType::Timestamp);
} 