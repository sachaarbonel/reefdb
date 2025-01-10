use std::time::SystemTime;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum WALOperation {
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
    Commit,
    Rollback,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct WALEntry {
    pub transaction_id: u64,
    pub timestamp: SystemTime,
    pub operation: WALOperation,
    pub table_name: String,
    pub data: Vec<u8>,
} 