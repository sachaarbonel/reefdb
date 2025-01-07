use std::fs::{File, OpenOptions};
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;
use serde::{Serialize, Deserialize};

use crate::error::ReefDBError;

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct WALEntry {
    pub transaction_id: u64,
    pub timestamp: SystemTime,
    pub operation: WALOperation,
    pub table_name: String,
    pub data: Vec<u8>,
}

pub struct WriteAheadLog {
    file: File,
    current_position: u64,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;
        
        Ok(WriteAheadLog {
            file,
            current_position: 0,
        })
    }

    pub fn append_entry(&mut self, entry: WALEntry) -> Result<(), ReefDBError> {
        let serialized = bincode::serialize(&entry)
            .map_err(|e| ReefDBError::Other(format!("Failed to serialize WAL entry: {}", e)))?;
        
        let len = serialized.len() as u64;
        self.file.write_all(&len.to_le_bytes())
            .map_err(|e| ReefDBError::Other(format!("Failed to write WAL entry length: {}", e)))?;
        
        self.file.write_all(&serialized)
            .map_err(|e| ReefDBError::Other(format!("Failed to write WAL entry: {}", e)))?;
        
        self.file.flush()
            .map_err(|e| ReefDBError::Other(format!("Failed to flush WAL: {}", e)))?;
        
        self.current_position += 8 + len;
        Ok(())
    }

    pub fn read_entries(&mut self) -> Result<Vec<WALEntry>, ReefDBError> {
        self.file.seek(SeekFrom::Start(0))
            .map_err(|e| ReefDBError::Other(format!("Failed to seek WAL: {}", e)))?;
        
        let mut entries = Vec::new();
        let mut position = 0;
        
        while position < self.current_position {
            let mut len_bytes = [0u8; 8];
            self.file.read_exact(&mut len_bytes)
                .map_err(|e| ReefDBError::Other(format!("Failed to read WAL entry length: {}", e)))?;
            
            let len = u64::from_le_bytes(len_bytes);
            let mut entry_data = vec![0u8; len as usize];
            
            self.file.read_exact(&mut entry_data)
                .map_err(|e| ReefDBError::Other(format!("Failed to read WAL entry: {}", e)))?;
            
            let entry: WALEntry = bincode::deserialize(&entry_data)
                .map_err(|e| ReefDBError::Other(format!("Failed to deserialize WAL entry: {}", e)))?;
            
            entries.push(entry);
            position += 8 + len;
        }
        
        Ok(entries)
    }

    pub fn truncate(&mut self) -> Result<(), ReefDBError> {
        self.file.set_len(0)
            .map_err(|e| ReefDBError::Other(format!("Failed to truncate WAL: {}", e)))?;
        self.current_position = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_operations() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        
        let mut wal = WriteAheadLog::new(&wal_path).unwrap();
        
        let entry = WALEntry {
            transaction_id: 1,
            timestamp: SystemTime::now(),
            operation: WALOperation::Insert,
            table_name: "users".to_string(),
            data: vec![1, 2, 3],
        };
        
        wal.append_entry(entry).unwrap();
        
        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        
        match entries[0].operation {
            WALOperation::Insert => (),
            _ => panic!("Unexpected operation type"),
        }
        
        assert_eq!(entries[0].transaction_id, 1);
        assert_eq!(entries[0].table_name, "users");
        assert_eq!(entries[0].data, vec![1, 2, 3]);
    }
} 