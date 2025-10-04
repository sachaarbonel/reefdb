use serde::{Deserialize, Serialize};

use crate::error::ReefDBError;
use crate::fts::search::Search;
use crate::indexes::index_manager::IndexType;
use crate::sql::column_def::ColumnDef;
use crate::sql::data_type::DataType;
use crate::sql::data_value::DataValue;
use crate::storage::Storage;
use crate::{indexes::index_manager::IndexManager, ReefDB};

pub type CommandId = u128;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct CommandBatch {
    pub id: CommandId,
    pub commands: Vec<ReplicatedCommand>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ReplicatedCommand {
    CreateTable { name: String, columns: Vec<ColumnDef> },
    DropTable { name: String },
    InsertRow { table: String, values: Vec<DataValue> },
    UpdateRows {
        table: String,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<(String, DataValue)>,
    },
    DeleteRows { table: String, where_clause: Option<(String, DataValue)> },
    CreateIndex { table: String, column: String },
    DropIndex { table: String, column: String },
    AlterAddColumn { table: String, column_def: ColumnDef },
    AlterDropColumn { table: String, column_name: String },
    AlterRenameColumn { table: String, old_name: String, new_name: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ApplyOutcome {
    CreateTable,
    DropTable,
    Insert { row_id: usize },
    Update { updated: usize },
    Delete { deleted: usize },
    CreateIndex,
    DropIndex,
    AlterTable,
}

impl<S, FTS> ReefDB<S, FTS>
where
    S: Storage + IndexManager + Clone + std::any::Any,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub(crate) fn next_command_id(&mut self) -> CommandId {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.saturating_add(1);
        id
    }

    pub fn apply_new(&mut self, cmd: ReplicatedCommand) -> Result<ApplyOutcome, ReefDBError> {
        let id = self.next_command_id();
        self.apply(id, cmd)
    }

    pub fn apply(&mut self, id: CommandId, cmd: ReplicatedCommand) -> Result<ApplyOutcome, ReefDBError> {
        if let Some(outcome) = self.applied_commands.get(&id) {
            return Ok(outcome.clone());
        }

        let outcome = match cmd {
            ReplicatedCommand::CreateTable { name, columns } => {
                if columns.is_empty() {
                    return Err(ReefDBError::Other("Cannot create table with empty column list".to_string()));
                }

                if self.storage.table_exists(&name) || self.tables.table_exists(&name) {
                    return Err(ReefDBError::Other(format!("Table {} already exists", name)));
                }

                self.storage.insert_table(name.clone(), columns.clone(), vec![]);
                self.tables.insert_table(name.clone(), columns.clone(), vec![]);

                for column in columns.iter() {
                    if column.data_type == DataType::TSVector {
                        self.inverted_index.add_column(&name, &column.name);
                    }
                }

                if !self.storage.table_exists(&name) || !self.tables.table_exists(&name) {
                    return Err(ReefDBError::Other("Failed to create table".to_string()));
                }

                ApplyOutcome::CreateTable
            }
            ReplicatedCommand::DropTable { name } => {
                if !self.storage.table_exists(&name) {
                    return Err(ReefDBError::TableNotFound(name));
                }
                self.storage.drop_table(&name);
                self.tables.drop_table(&name);
                ApplyOutcome::DropTable
            }
            ReplicatedCommand::InsertRow { table, values } => {
                // Validate schema and types
                let schema = {
                    let (schema, _) = self
                        .storage
                        .get_table_ref(&table)
                        .ok_or_else(|| ReefDBError::TableNotFound(table.clone()))?;
                    schema.clone()
                };

                if values.len() != schema.len() {
                    return Err(ReefDBError::Other(format!(
                        "Number of values ({}) does not match number of columns ({})",
                        values.len(),
                        schema.len()
                    )));
                }
                for (value, column) in values.iter().zip(schema.iter()) {
                    if !value.matches_type(&column.data_type) {
                        return Err(ReefDBError::Other(format!(
                            "Value type mismatch for column {}: expected {:?}, got {:?}",
                            column.name, column.data_type, value
                        )));
                    }
                }

                let row_id = self.storage.push_value(&table, values.clone())?;
                self.tables.push_value(&table, values.clone())?;

                for (i, col) in schema.iter().enumerate() {
                    if col.data_type == DataType::TSVector {
                        if let DataValue::Text(text) = &values[i] {
                            self.inverted_index.add_document(&table, &col.name, row_id, text);
                        }
                    }
                }

                ApplyOutcome::Insert { row_id }
            }
            ReplicatedCommand::UpdateRows { table, updates, where_clause } => {
                // Validate table and columns
                let (schema, _) = self
                    .storage
                    .get_table_ref(&table)
                    .ok_or_else(|| ReefDBError::TableNotFound(table.clone()))?;

                for (col_name, value) in &updates {
                    let column = schema
                        .iter()
                        .find(|c| &c.name == col_name)
                        .ok_or_else(|| ReefDBError::ColumnNotFound(col_name.clone()))?;
                    if !value.matches_type(&column.data_type) {
                        return Err(ReefDBError::Other(format!(
                            "Value type mismatch for column {}: expected {:?}, got {:?}",
                            col_name, column.data_type, value
                        )));
                    }
                }

                // Apply to storage (authoritative) and rely on future snapshot/restore to refresh in-memory tables
                let updated = self.storage.update_table(&table, updates, where_clause);
                ApplyOutcome::Update { updated }
            }
            ReplicatedCommand::DeleteRows { table, where_clause } => {
                // Validate table
                if !self.storage.table_exists(&table) {
                    return Err(ReefDBError::TableNotFound(table));
                }
                let deleted = self.storage.delete_table(&table, where_clause);
                ApplyOutcome::Delete { deleted }
            }
            ReplicatedCommand::CreateIndex { table, column } => {
                let (schema, _) = self
                    .storage
                    .get_table_ref(&table)
                    .ok_or_else(|| ReefDBError::TableNotFound(table.clone()))?;
                if !schema.iter().any(|c| c.name == column) {
                    return Err(ReefDBError::ColumnNotFound(column));
                }
                let btree = crate::indexes::btree::BTreeIndex::new();
                self.storage.create_index(&table, &column, IndexType::BTree(btree))?;
                ApplyOutcome::CreateIndex
            }
            ReplicatedCommand::DropIndex { table, column } => {
                let (schema, _) = self
                    .storage
                    .get_table_ref(&table)
                    .ok_or_else(|| ReefDBError::TableNotFound(table.clone()))?;
                if !schema.iter().any(|c| c.name == column) {
                    return Err(ReefDBError::ColumnNotFound(column));
                }
                self.storage.drop_index(&table, &column);
                ApplyOutcome::DropIndex
            }
            ReplicatedCommand::AlterAddColumn { table, column_def } => {
                let (schema, _) = self
                    .storage
                    .get_table_ref(&table)
                    .ok_or_else(|| ReefDBError::TableNotFound(table.clone()))?;
                if schema.iter().any(|c| c.name == column_def.name) {
                    return Err(ReefDBError::Other(format!(
                        "Column {} already exists in table {}",
                        column_def.name, table
                    )));
                }
                self.storage.add_column(&table, column_def)?;
                ApplyOutcome::AlterTable
            }
            ReplicatedCommand::AlterDropColumn { table, column_name } => {
                if !self.storage.table_exists(&table) {
                    return Err(ReefDBError::TableNotFound(table));
                }
                self.storage.drop_column(&table, &column_name)?;
                ApplyOutcome::AlterTable
            }
            ReplicatedCommand::AlterRenameColumn { table, old_name, new_name } => {
                let (schema, _) = self
                    .storage
                    .get_table_ref(&table)
                    .ok_or_else(|| ReefDBError::TableNotFound(table.clone()))?;
                if schema.iter().any(|c| c.name == new_name) {
                    return Err(ReefDBError::Other(format!(
                        "Column {} already exists in table {}",
                        new_name, table
                    )));
                }
                self.storage.rename_column(&table, &old_name, &new_name)?;
                ApplyOutcome::AlterTable
            }
        };

        self.applied_commands.insert(id, outcome.clone());
        Ok(outcome)
    }

    pub fn apply_batch(&mut self, batch: CommandBatch) -> Result<Vec<ApplyOutcome>, ReefDBError> {
        let mut outcomes = Vec::with_capacity(batch.commands.len());
        for (i, cmd) in batch.commands.into_iter().enumerate() {
            // Derive a deterministic per-command id from batch id to ensure idempotency across replay
            let cmd_id = batch.id.saturating_add(i as u128);
            let outcome = self.apply(cmd_id, cmd)?;
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }
}


