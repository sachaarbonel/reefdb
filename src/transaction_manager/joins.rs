use std::any::Any;
use std::cmp::Ordering;

use crate::fts::search::Search;
use crate::indexes::index_manager::IndexManager;
use crate::sql::clauses::join_clause::JoinClause;
use crate::sql::clauses::order_by::{OrderByClause, OrderDirection};
use crate::sql::clauses::wheres::where_type::WhereType;
use crate::sql::column::Column;
use crate::sql::column_def::ColumnDef;
use crate::sql::column_value_pair::ColumnValuePair;
use crate::sql::data_value::DataValue;
use crate::storage::Storage;

use super::TransactionManager;

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub(super) fn evaluate_where_clause(
        where_clause: &WhereType,
        row_data: &[DataValue],
        schema: &[ColumnDef],
        table_name: &str,
    ) -> bool {
        match where_clause {
            WhereType::Regular(clause) => {
                // Find the column in the schema
                let col_idx = if let Some(ref clause_table) = clause.table {
                    // If table is specified, only look in that table's columns
                    if clause_table == table_name {
                        schema.iter().position(|c| c.name == clause.col_name)
                    } else {
                        // If the table doesn't match, we might be looking at joined data
                        // In this case, we need to look through all columns
                        schema.iter().position(|c| c.name == clause.col_name)
                    }
                } else {
                    // If no table specified, look in all columns
                    schema.iter().position(|c| c.name == clause.col_name)
                };

                if let Some(idx) = col_idx {
                    clause.operator.evaluate(&row_data[idx], &clause.value)
                } else {
                    false
                }
            },
            WhereType::FTS(_) => {
                // FTS search is handled separately by the FTS index
                false
            },
            WhereType::And(left, right) => {
                Self::evaluate_where_clause(left, row_data, schema, table_name) &&
                Self::evaluate_where_clause(right, row_data, schema, table_name)
            },
            WhereType::Or(left, right) => {
                Self::evaluate_where_clause(left, row_data, schema, table_name) ||
                Self::evaluate_where_clause(right, row_data, schema, table_name)
            },
        }
    }

    pub(super) fn evaluate_join_condition(
        condition: &(ColumnValuePair, ColumnValuePair),
        left_data: &[DataValue],
        left_schema: &[ColumnDef],
        right_data: &[DataValue],
        right_schema: &[ColumnDef],
        left_table: &str,
        right_table: &str,
    ) -> bool {
        let (left_pair, right_pair) = condition;

        // Get values from both tables
        let left_value = if left_pair.table_name.is_empty() || left_pair.table_name == left_table {
            if let Some(idx) = left_schema.iter().position(|c| c.name == left_pair.column_name) {
                Some(&left_data[idx])
            } else {
                None
            }
        } else if left_pair.table_name == right_table {
            if let Some(idx) = right_schema.iter().position(|c| c.name == left_pair.column_name) {
                Some(&right_data[idx])
            } else {
                None
            }
        } else {
            None
        };

        let right_value = if right_pair.table_name.is_empty() || right_pair.table_name == left_table {
            if let Some(idx) = left_schema.iter().position(|c| c.name == right_pair.column_name) {
                Some(&left_data[idx])
            } else {
                None
            }
        } else if right_pair.table_name == right_table {
            if let Some(idx) = right_schema.iter().position(|c| c.name == right_pair.column_name) {
                Some(&right_data[idx])
            } else {
                None
            }
        } else {
            None
        };

        // Compare the values if both were found
        if let (Some(left_val), Some(right_val)) = (left_value, right_value) {
            left_val == right_val
        } else {
            false
        }
    }

    pub(super) fn sort_results(
        &self,
        mut results: Vec<(usize, Vec<DataValue>)>,
        order_by: &[OrderByClause],
        schema: &[ColumnDef],
        table_name: &str,
        joined_tables: &[(JoinClause, (Vec<ColumnDef>, Vec<Vec<DataValue>>))],
    ) -> Vec<(usize, Vec<DataValue>)> {
        if order_by.is_empty() || results.is_empty() {
            return results;
        }

        results.sort_by(|a, b| {
            for order_clause in order_by {
                let col_name = &order_clause.column.name;

                // Find the column index in the result values
                let col_idx = match &order_clause.column.table {
                    Some(table) => {
                        // For columns with explicit table references
                        if table == table_name {
                            // Column is from the main table
                            schema.iter().position(|c| c.name == *col_name)
                        } else {
                            // Column is from a joined table
                            joined_tables.iter()
                                .find(|(join, _)| join.table_ref.name == *table)
                                .and_then(|(_, (schema, _))| schema.iter().position(|c| c.name == *col_name))
                                .map(|pos| pos + schema.len())
                        }
                    },
                    None => {
                        // For columns without table references, find the first matching column
                        schema.iter().position(|c| c.name == *col_name).or_else(|| {
                            joined_tables.iter()
                                .find_map(|(_, (schema, _))| {
                                    schema.iter().position(|c| c.name == *col_name)
                                        .map(|pos| pos + schema.len())
                                })
                        })
                    }
                };

                if let Some(idx) = col_idx {
                    if idx < a.1.len() && idx < b.1.len() {
                        let cmp = a.1[idx].cmp(&b.1[idx]);
                        if cmp != Ordering::Equal {
                            return match order_clause.direction {
                                OrderDirection::Desc => cmp.reverse(),
                                OrderDirection::Asc => cmp,
                            };
                        }
                    }
                }
            }
            Ordering::Equal
        });

        results
    }

    pub(super) fn project_results(
        &self,
        results: Vec<(usize, Vec<DataValue>)>,
        columns: &[Column],
        schema: &[ColumnDef],
        table_name: &str,
        joined_tables: &[(JoinClause, (Vec<ColumnDef>, Vec<Vec<DataValue>>))],
    ) -> Vec<(usize, Vec<DataValue>)> {
        let mut projected_results = Vec::new();
        for (i, joined_data) in results {
            let mut projected = Vec::new();
            if columns.iter().any(|c| c.name == "*") {
                projected = joined_data;
            } else {
                for col in columns {
                    let col_value = if let Some(table) = &col.table {
                        // Find column in specific table's schema
                        let (schema_start, schema_len) = if table == table_name {
                            (0, schema.len())
                        } else {
                            let mut start = schema.len();
                            let mut found = false;
                            let mut len = 0;
                            for (join, (join_schema, _)) in joined_tables {
                                if &join.table_ref.name == table {
                                    len = join_schema.len();
                                    found = true;
                                    break;
                                }
                                start += join_schema.len();
                            }
                            if !found {
                                (0, 0) // Table not found
                            } else {
                                (start, len)
                            }
                        };

                        // Ensure we don't exceed the data boundaries
                        if schema_start < joined_data.len() {
                            let end = std::cmp::min(schema_start + schema_len, joined_data.len());
                            let schema_slice = if schema_start < schema.len() {
                                &schema[schema_start..std::cmp::min(schema_start + schema_len, schema.len())]
                            } else {
                                for (join, (join_schema, _)) in joined_tables {
                                    if &join.table_ref.name == table {
                                        if let Some(idx) = join_schema.iter().position(|c| c.name == col.name) {
                                            if schema_start + idx < joined_data.len() {
                                                projected.push(joined_data[schema_start + idx].clone());
                                            }
                                            break;
                                        }
                                    }
                                }
                                &[]
                            };

                            if let Some(idx) = schema_slice.iter().position(|c| c.name == col.name) {
                                Some(joined_data[schema_start + idx].clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        // Try to find column in any table
                        if let Some(idx) = schema.iter().position(|c| c.name == col.name) {
                            Some(joined_data[idx].clone())
                        } else {
                            // Try joined tables
                            let mut start = schema.len();
                            for (_, (join_schema, _)) in joined_tables {
                                if let Some(idx) = join_schema.iter().position(|c| c.name == col.name) {
                                    if start + idx < joined_data.len() {
                                        projected.push(joined_data[start + idx].clone());
                                        break;
                                    }
                                }
                                start += join_schema.len();
                            }
                            None
                        }
                    };

                    if let Some(value) = col_value {
                        projected.push(value);
                    }
                }
            }
            projected_results.push((i, projected));
        }

        projected_results
    }
}
