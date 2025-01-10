use std::fs::{File, OpenOptions};
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;
use bincode;

use crate::error::ReefDBError;
use super::entry::WALEntry;

pub struct WriteAheadLog {
    file: File,
    current_position: u64,
    sync_on_append: bool,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;
        
        let current_position = file.metadata()?.len();
        
        Ok(WriteAheadLog {
            file,
            current_position,
            sync_on_append: true,
        })
    }

    pub fn new_in_memory() -> io::Result<Self> {
        let file = tempfile::tempfile()?;
        Ok(WriteAheadLog {
            file,
            current_position: 0,
            sync_on_append: true,
        })
    }

    pub fn set_sync_on_append(&mut self, sync: bool) {
        self.sync_on_append = sync;
    }

    pub fn append_entry(&mut self, entry: WALEntry) -> Result<(), ReefDBError> {
        let serialized = bincode::serialize(&entry)
            .map_err(|e| ReefDBError::WALError(format!("Failed to serialize WAL entry: {}", e)))?;
        
        let len = serialized.len() as u64;
        self.file.write_all(&len.to_le_bytes())
            .map_err(|e| ReefDBError::WALError(format!("Failed to write WAL entry length: {}", e)))?;
        
        self.file.write_all(&serialized)
            .map_err(|e| ReefDBError::WALError(format!("Failed to write WAL entry: {}", e)))?;
        
        self.file.flush()
            .map_err(|e| ReefDBError::WALError(format!("Failed to flush WAL: {}", e)))?;
        
        if self.sync_on_append {
            self.file.sync_all()
                .map_err(|e| ReefDBError::WALError(format!("Failed to sync WAL to disk: {}", e)))?;
        }
        
        self.current_position += 8 + len;
        Ok(())
    }

    pub fn read_entries(&mut self) -> Result<Vec<WALEntry>, ReefDBError> {
        self.file.seek(SeekFrom::Start(0))
            .map_err(|e| ReefDBError::WALError(format!("Failed to seek WAL: {}", e)))?;
        
        let mut entries = Vec::new();
        let mut position = 0;
        
        while position < self.current_position {
            let mut len_bytes = [0u8; 8];
            self.file.read_exact(&mut len_bytes)
                .map_err(|e| ReefDBError::WALError(format!("Failed to read WAL entry length: {}", e)))?;
            
            let len = u64::from_le_bytes(len_bytes);
            let mut entry_data = vec![0u8; len as usize];
            
            self.file.read_exact(&mut entry_data)
                .map_err(|e| ReefDBError::WALError(format!("Failed to read WAL entry: {}", e)))?;
            
            let entry: WALEntry = bincode::deserialize(&entry_data)
                .map_err(|e| ReefDBError::WALError(format!("Failed to deserialize WAL entry: {}", e)))?;
            
            entries.push(entry);
            position += 8 + len;
        }
        
        Ok(entries)
    }

    pub fn truncate(&mut self) -> Result<(), ReefDBError> {
        self.file.set_len(0)
            .map_err(|e| ReefDBError::WALError(format!("Failed to truncate WAL: {}", e)))?;
        
        if self.sync_on_append {
            self.file.sync_all()
                .map_err(|e| ReefDBError::WALError(format!("Failed to sync WAL after truncate: {}", e)))?;
        }
        
        self.current_position = 0;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), ReefDBError> {
        self.file.sync_all()
            .map_err(|e| ReefDBError::WALError(format!("Failed to sync WAL to disk: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use crate::wal::entry::WALOperation;
    use tempfile::tempdir;

    fn create_test_entry(id: u64, operation: WALOperation) -> WALEntry {
        WALEntry {
            transaction_id: id,
            timestamp: SystemTime::now(),
            operation,
            table_name: format!("table_{}", id),
            data: vec![id as u8],
        }
    }

    #[test]
    fn test_single_entry() {
        let mut wal = WriteAheadLog::new_in_memory().unwrap();
        let entry = create_test_entry(1, WALOperation::Insert);
        
        wal.append_entry(entry.clone()).unwrap();
        let entries = wal.read_entries().unwrap();
        
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].transaction_id, entry.transaction_id);
        assert_eq!(entries[0].table_name, entry.table_name);
        assert_eq!(entries[0].data, entry.data);
    }

    #[test]
    fn test_multiple_entries() {
        let mut wal = WriteAheadLog::new_in_memory().unwrap();
        
        let entries = vec![
            create_test_entry(1, WALOperation::Insert),
            create_test_entry(2, WALOperation::Update),
            create_test_entry(3, WALOperation::Delete),
        ];
        
        for entry in entries.iter() {
            wal.append_entry(entry.clone()).unwrap();
        }
        
        let read_entries = wal.read_entries().unwrap();
        assert_eq!(read_entries.len(), 3);
        
        for (original, read) in entries.iter().zip(read_entries.iter()) {
            assert_eq!(read.transaction_id, original.transaction_id);
            assert_eq!(read.operation, original.operation);
            assert_eq!(read.table_name, original.table_name);
            assert_eq!(read.data, original.data);
        }
    }

    #[test]
    fn test_truncate() {
        let mut wal = WriteAheadLog::new_in_memory().unwrap();
        
        // Add some entries
        for i in 1..=3 {
            wal.append_entry(create_test_entry(i, WALOperation::Insert)).unwrap();
        }
        
        // Verify entries were written
        assert_eq!(wal.read_entries().unwrap().len(), 3);
        
        // Truncate and verify it's empty
        wal.truncate().unwrap();
        assert_eq!(wal.read_entries().unwrap().len(), 0);
        
        // Verify we can still write after truncate
        wal.append_entry(create_test_entry(4, WALOperation::Insert)).unwrap();
        assert_eq!(wal.read_entries().unwrap().len(), 1);
    }

    #[test]
    fn test_sync_on_append() {
        let mut wal = WriteAheadLog::new_in_memory().unwrap();
        
        // Test with sync_on_append enabled (default)
        assert!(wal.sync_on_append);
        wal.append_entry(create_test_entry(1, WALOperation::Insert)).unwrap();
        
        // Test with sync_on_append disabled
        wal.set_sync_on_append(false);
        assert!(!wal.sync_on_append);
        wal.append_entry(create_test_entry(2, WALOperation::Insert)).unwrap();
        
        // Verify both entries were written correctly
        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_file_based_wal() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.wal");
        
        // Create and write to WAL
        {
            let mut wal = WriteAheadLog::new(&file_path).unwrap();
            wal.append_entry(create_test_entry(1, WALOperation::Insert)).unwrap();
            wal.append_entry(create_test_entry(2, WALOperation::Update)).unwrap();
        }
        
        // Open existing WAL and verify contents
        {
            let mut wal = WriteAheadLog::new(&file_path).unwrap();
            let entries = wal.read_entries().unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].transaction_id, 1);
            assert_eq!(entries[1].transaction_id, 2);
        }
    }

    #[test]
    fn test_large_entries() {
        let mut wal = WriteAheadLog::new_in_memory().unwrap();
        
        // Create an entry with large data (1MB)
        let large_data = vec![42u8; 1024 * 1024];
        let entry = WALEntry {
            transaction_id: 1,
            timestamp: SystemTime::now(),
            operation: WALOperation::Insert,
            table_name: "large_table".to_string(),
            data: large_data.clone(),
        };
        
        wal.append_entry(entry).unwrap();
        let entries = wal.read_entries().unwrap();
        
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data.len(), 1024 * 1024);
        assert_eq!(entries[0].data, large_data);
    }

    #[test]
    fn test_persistence_after_sync() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("sync_test.wal");
        
        // Write entries with sync
        {
            let mut wal = WriteAheadLog::new(&file_path).unwrap();
            wal.append_entry(create_test_entry(1, WALOperation::Insert)).unwrap();
            wal.sync().unwrap();
        }
        
        // Verify entries persist after sync
        {
            let mut wal = WriteAheadLog::new(&file_path).unwrap();
            let entries = wal.read_entries().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].transaction_id, 1);
        }
    }

    #[test]
    fn test_invalid_file_path() {
        let result = WriteAheadLog::new("/nonexistent/directory/test.wal");
        assert!(result.is_err());
    }
} 