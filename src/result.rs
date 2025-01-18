use std::ops::Index;

use crate::sql::data_value::DataValue;
use crate::sql::data_type::DataType;
use crate::sql::column::Column;
use crate::sql::column_def::ColumnDef;
use crate::sql::constraints::constraint::Constraint;
use crate::sql::column::ColumnType;
use crate::error::ReefDBError;

#[derive(PartialEq, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table: Option<String>,
    pub nullable: bool,
}

impl ColumnInfo {
    pub fn from_schema_and_columns(
        schema: &[ColumnDef],
        columns: &[Column],
        table_name: &str,
    ) -> Result<Vec<ColumnInfo>, ReefDBError> {
        if columns.iter().any(|c| c.name == "*") {
            // If selecting all columns, include all from schema
            Ok(schema.iter().map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                table: Some(table_name.to_string()),
                nullable: col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
            }).collect())
        } else {
            // Only include selected columns
            columns.iter().map(|col| {
                match &col.column_type {
                    ColumnType::Regular(name) => {
                        let schema_col = schema.iter()
                            .find(|c| c.name == col.name)
                            .ok_or_else(|| ReefDBError::ColumnNotFound(col.name.clone()))?;
                        Ok(ColumnInfo {
                            name: col.name.clone(),
                            data_type: schema_col.data_type.clone(),
                            table: col.table.clone().or_else(|| Some(table_name.to_string())),
                            nullable: schema_col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
                        })
                    },
                    ColumnType::Function(name, args) => {
                        // For function-generated columns, assume they are nullable and use Float type for ranking functions
                        Ok(ColumnInfo {
                            name: col.name.clone(),
                            data_type: DataType::Float,
                            table: None,
                            nullable: true,
                        })
                    },
                    ColumnType::Wildcard => unreachable!("Wildcard should be handled by the first branch"),
                }
            }).collect()
        }
    }

    pub fn from_joined_schemas(
        main_schema: &[ColumnDef],
        main_table: &str,
        joined_tables: &[(&str, &[ColumnDef])],
        columns: &[Column],
    ) -> Result<Vec<ColumnInfo>, ReefDBError> {
        if columns.iter().any(|c| c.name == "*") {
            // If selecting all columns, include all from all schemas
            let mut all_columns = Vec::new();
            
            // Add main table columns
            all_columns.extend(main_schema.iter().map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                table: Some(main_table.to_string()),
                nullable: col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
            }));

            // Add joined table columns
            for (table_name, schema) in joined_tables {
                all_columns.extend(schema.iter().map(|col| ColumnInfo {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    table: Some(table_name.to_string()),
                    nullable: col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
                }));
            }

            Ok(all_columns)
        } else {
            // Only include selected columns
            columns.iter().map(|col| {
                match &col.column_type {
                    ColumnType::Regular(name) => {
                        if let Some(table) = &col.table {
                            if table == main_table {
                                let schema_col = main_schema.iter()
                                    .find(|c| c.name == col.name)
                                    .ok_or_else(|| ReefDBError::ColumnNotFound(col.name.clone()))?;
                                Ok(ColumnInfo {
                                    name: col.name.clone(),
                                    data_type: schema_col.data_type.clone(),
                                    table: Some(table.clone()),
                                    nullable: schema_col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
                                })
                            } else if let Some((_, schema)) = joined_tables.iter().find(|(t, _)| t == table) {
                                let schema_col = schema.iter()
                                    .find(|c| c.name == col.name)
                                    .ok_or_else(|| ReefDBError::ColumnNotFound(col.name.clone()))?;
                                Ok(ColumnInfo {
                                    name: col.name.clone(),
                                    data_type: schema_col.data_type.clone(),
                                    table: Some(table.clone()),
                                    nullable: schema_col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
                                })
                            } else {
                                Err(ReefDBError::TableNotFound(table.clone()))
                            }
                        } else {
                            // Try to find column in main schema first
                            if let Some(schema_col) = main_schema.iter().find(|c| c.name == col.name) {
                                Ok(ColumnInfo {
                                    name: col.name.clone(),
                                    data_type: schema_col.data_type.clone(),
                                    table: Some(main_table.to_string()),
                                    nullable: schema_col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
                                })
                            } else {
                                // Try joined tables
                                for (table_name, schema) in joined_tables {
                                    if let Some(schema_col) = schema.iter().find(|c| c.name == col.name) {
                                        return Ok(ColumnInfo {
                                            name: col.name.clone(),
                                            data_type: schema_col.data_type.clone(),
                                            table: Some(table_name.to_string()),
                                            nullable: schema_col.constraints.iter().all(|c| !matches!(c, Constraint::NotNull)),
                                        });
                                    }
                                }
                                Err(ReefDBError::ColumnNotFound(col.name.clone()))
                            }
                        }
                    },
                    ColumnType::Function(name, args) => {
                        // For function-generated columns, assume they are nullable and use Float type for ranking functions
                        Ok(ColumnInfo {
                            name: col.name.clone(),
                            data_type: DataType::Float,
                            table: None,
                            nullable: true,
                        })
                    },
                    ColumnType::Wildcard => unreachable!("Wildcard should be handled by the first branch"),
                }
            }).collect()
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<(usize, Vec<DataValue>)>,
    pub row_count: usize,
}

// impl index
impl Index<usize> for QueryResult {
    type Output = Vec<DataValue>;
    fn index(&self, index: usize) -> &Self::Output {
        &self.rows[index].1
    }
}

impl QueryResult {
    pub fn new(result: Vec<(usize, Vec<DataValue>)>) -> Self {
        let row_count = result.len();
        QueryResult { 
            columns: Vec::new(),
            rows: result, 
            row_count 
        }
    }

    pub fn with_columns(result: Vec<(usize, Vec<DataValue>)>, columns: Vec<ColumnInfo>) -> Self {
        let row_count = result.len();
        QueryResult { 
            columns,
            rows: result, 
            row_count 
        }
    }

    pub fn len(&self) -> usize {
        self.row_count
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column(&self, index: usize) -> Option<&ColumnInfo> {
        self.columns.get(index)
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|col| col.name == name)
    }
}

#[derive(PartialEq, Debug)]
pub enum ReefDBResult {
    Select(QueryResult),
    Insert(usize),
    CreateTable,
    Update(usize),
    Delete(usize),
    AlterTable,
    DropTable,
    CreateIndex,
    DropIndex,
    Savepoint,
    RollbackToSavepoint,
    ReleaseSavepoint,
    BeginTransaction,
    Commit,
}
