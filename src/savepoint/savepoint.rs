use std::time::SystemTime;
use crate::storage::TableStorage;

#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    pub table_snapshot: TableStorage,
    pub timestamp: SystemTime,
    pub state: SavepointState,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SavepointState {
    Active,
    Released,
    RolledBack,
} 