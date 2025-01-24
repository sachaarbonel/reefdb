mod savepoint_handler;
mod state_handler;

pub use savepoint_handler::SavepointHandler;
pub use state_handler::{TransactionState, TransactionStateHandler, IsolationLevel};
use crate::fts::search::Search;
use crate::sql::clauses::order_by;
use crate::{
    error::ReefDBError,
    result::ReefDBResult,
    storage::Storage,
    indexes::index_manager::IndexManager,
    sql::{
        statements::{
            Statement,
            select::SelectStatement,
            create::CreateStatement,
            insert::InsertStatement,
            update::UpdateStatement,
            delete::DeleteStatement,
            alter::AlterStatement,
            drop::DropStatement,
        },
    },
   
    ReefDB,
    acid::AcidManager,
    TableStorage,
    savepoint::{Savepoint, SavepointState},
};

#[derive(Clone)]
pub struct Transaction<S: Storage + IndexManager + Clone, FTS: Search + Clone>
where
    FTS::NewArgs: Clone,
{
    pub(crate) state_handler: TransactionStateHandler,
    pub(crate) savepoint_handler: SavepointHandler,
    pub(crate) reef_db: ReefDB<S, FTS>,
    pub(crate) acid_manager: AcidManager,
}

impl<S: Storage + IndexManager + Clone, FTS: Search + Clone> Transaction<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn create(reef_db: ReefDB<S, FTS>, isolation_level: IsolationLevel) -> Self {
        let id = rand::random::<u64>();
        let state_handler = TransactionStateHandler::new(id, isolation_level);
        let savepoint_handler = SavepointHandler::new();
        let acid_manager = AcidManager::new(reef_db.tables.clone(), isolation_level);

        let mut transaction = Transaction {
            state_handler,
            savepoint_handler,
            reef_db: reef_db.clone(),
            acid_manager,
        };

        // Take initial snapshot
        transaction.acid_manager.begin_atomic(&reef_db.tables);

        transaction
    }

    pub fn create_savepoint(&mut self, name: String) -> Result<(), ReefDBError> {
        if *self.state_handler.get_state() != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        // Create a deep copy of the current state
        let mut storage_state = TableStorage::new();
        
        // First, get the base state from storage
        for (table_name, (columns, rows)) in self.reef_db.storage.get_all_tables().iter() {
            storage_state.tables.insert(table_name.clone(), (columns.clone(), rows.clone()));
        }
        
        // Then, apply all changes from the transaction's view
        for (table_name, (columns, rows)) in self.reef_db.tables.tables.iter() {
            if rows.is_empty() {
                storage_state.tables.remove(table_name);
            } else {
                storage_state.tables.insert(table_name.clone(), (columns.clone(), rows.clone()));
            }
        }
        
        self.savepoint_handler.create_savepoint(name, storage_state)
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), ReefDBError> {
        if *self.state_handler.get_state() != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        let (snapshot, _) = self.savepoint_handler.rollback_to_savepoint(name)?;
        
        // Now restore the database state
        self.reef_db.tables = TableStorage::new();
        self.reef_db.tables.restore_from(&snapshot);
        
        // Clear and restore storage state
        self.reef_db.storage.clear();
        for (table_name, (columns, rows)) in snapshot.tables.iter() {
            if !rows.is_empty() {
                self.reef_db.storage.insert_table(table_name.clone(), columns.clone(), rows.clone());
            }
        }
        
        // Update the ACID manager's snapshot
        let mut current_state = self.acid_manager.get_committed_snapshot();
        current_state.restore_from(&snapshot);
        self.acid_manager.begin_atomic(&current_state);
        
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<(), ReefDBError> {
        if *self.state_handler.get_state() != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        self.savepoint_handler.release_savepoint(name)
    }

    pub fn commit(&mut self, reef_db: &mut ReefDB<S, FTS>) -> Result<(), ReefDBError> {
        if *self.state_handler.get_state() != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }

        // For serializable isolation, we need to ensure we're working with the correct snapshot
        if self.state_handler.get_isolation_level() == IsolationLevel::Serializable {
            // First commit changes to ACID manager
            self.acid_manager.commit()?;

            // Get our snapshot from the start of the transaction
            let snapshot = self.acid_manager.get_committed_snapshot();
            
            // Apply our changes to the snapshot
            let mut final_state = snapshot.clone();
            final_state.restore_from(&self.reef_db.tables);

            // Update only the storage with our changes, not the in-memory tables
            reef_db.storage.restore_from(&final_state);
        } else {
            // For other isolation levels, commit normally
            self.acid_manager.commit()?;

            // Get the final state from ACID manager
            let final_state = self.acid_manager.get_committed_snapshot();

            // Update both storage and tables
            reef_db.tables.restore_from(&final_state);
            reef_db.storage.restore_from(&final_state);
        }

        // Update transaction state
        self.state_handler.commit()?;

        Ok(())
    }

    pub fn rollback(&mut self, reef_db: &mut ReefDB<S, FTS>) -> Result<(), ReefDBError> {
        if *self.state_handler.get_state() != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }

        // Rollback to the initial snapshot
        let snapshot = self.acid_manager.rollback_atomic();
        reef_db.tables.restore_from(&snapshot);
        self.reef_db.tables.restore_from(&snapshot);
        
        self.state_handler.rollback()?;
        Ok(())
    }

    pub fn get_state(&self) -> &TransactionState {
        self.state_handler.get_state()
    }

    pub fn get_id(&self) -> u64 {
        self.state_handler.get_id()
    }

    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.state_handler.get_isolation_level()
    }

    pub fn get_start_timestamp(&self) -> std::time::SystemTime {
        self.state_handler.get_start_timestamp()
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        if *self.state_handler.get_state() != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        match stmt {
            Statement::Create(CreateStatement::Table(name, columns)) => {
                self.reef_db.handle_create(name, columns)
            },
            Statement::Select(SelectStatement::FromTable(table_name, columns, where_clause, joins, order_by)) => {
                self.reef_db.handle_select(table_name, columns, where_clause, joins, order_by)
            },
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                self.reef_db.handle_insert(table_name, values)
            },
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, where_clause)) => {
                self.reef_db.handle_update(table_name, updates, where_clause)
            },
            Statement::Delete(DeleteStatement::FromTable(table_name, where_clause)) => {
                self.reef_db.handle_delete(table_name, where_clause)
            },
            Statement::Alter(AlterStatement { table_name, alter_type }) => {
                self.reef_db.handle_alter(table_name, alter_type)
            },
            Statement::Drop(DropStatement { table_name }) => {
                self.reef_db.handle_drop(table_name)
            },
            Statement::CreateIndex(stmt) => {
                self.reef_db.handle_create_index(stmt)
            },
            Statement::DropIndex(stmt) => {
                self.reef_db.handle_drop_index(stmt)
            },
            Statement::Savepoint(sp_stmt) => {
                self.create_savepoint(sp_stmt.name)
                    .map(|_| ReefDBResult::Savepoint)
            },
            Statement::RollbackToSavepoint(name) => {
                self.rollback_to_savepoint(&name)
                    .map(|_| ReefDBResult::RollbackToSavepoint)
            },
            Statement::ReleaseSavepoint(name) => {
                self.release_savepoint(&name)
                    .map(|_| ReefDBResult::ReleaseSavepoint)
            },
            Statement::BeginTransaction => {
                Ok(ReefDBResult::BeginTransaction)
            },
            Statement::Commit => {
                Ok(ReefDBResult::Commit)
            },
        }
    }

    pub fn get_table_state(&self) -> TableStorage {
        self.reef_db.tables.clone()
    }

    pub fn restore_table_state(&mut self, state: &TableStorage) {
        self.reef_db.tables = TableStorage::new();
        self.reef_db.tables.restore_from(state);
    }
} 