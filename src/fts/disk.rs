use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;

use serde::Deserialize;
use serde::Serialize;

use crate::indexes::gin::GinIndex;
use super::search::Search;
use super::tokenizers::tokenizer::Tokenizer;
use super::tokenizers::default::DefaultTokenizer;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OnDiskInvertedIndex<T: Tokenizer> {
    index: GinIndex<T>,
    file_path: String,
}

impl<T: Tokenizer + Serialize + for<'de> Deserialize<'de>> OnDiskInvertedIndex<T> {
    pub fn load_from_file(&self, file_path: &str) -> io::Result<Self> {
        let mut file = File::open(file_path)?;
        let mut encoded_data = Vec::new();
        file.read_to_end(&mut encoded_data)?;
        let index = bincode::deserialize(&encoded_data)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        Ok(index)
    }

    pub fn save_to_file(&self, file_path: &str) -> io::Result<()> {
        let encoded_data = bincode::serialize(self)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let mut file = File::create(file_path)?;
        file.write_all(&encoded_data)
    }
}

impl<T: Tokenizer + Serialize + for<'de> Deserialize<'de>> Search for OnDiskInvertedIndex<T> {
    type NewArgs = String;

    fn new(args: Self::NewArgs) -> Self {
        let index = GinIndex::new();
        let file_path = args;
        let on_disk_index = OnDiskInvertedIndex {
            index,
            file_path: file_path.clone(),
        };
        on_disk_index
            .load_from_file(&file_path)
            .unwrap_or(on_disk_index)
    }

    fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize> {
        self.index.search(table, column, query)
    }

    fn add_column(&mut self, table: &str, column: &str) {
        self.index.add_column(table, column);
        self.save_to_file(&self.file_path).unwrap();
    }

    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        self.index.add_document(table, column, row_id, text);
        self.save_to_file(&self.file_path).unwrap();
    }

    fn remove_document(&mut self, table: &str, column: &str, row_id: usize) {
        self.index.remove_document(table, column, row_id);
        self.save_to_file(&self.file_path).unwrap();
    }

    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        self.index.update_document(table, column, row_id, text);
        self.save_to_file(&self.file_path).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::tokenizers::default::DefaultTokenizer;
    use tempfile::NamedTempFile;

    #[test]
    fn test_on_disk_inverted_index() {
        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
        let file_path = temp_file.into_temp_path().to_string_lossy().into_owned();

        let mut on_disk_index: OnDiskInvertedIndex<DefaultTokenizer> =
            OnDiskInvertedIndex::new(file_path);

        // Add documents
        on_disk_index.add_document("table1", "column1", 0, "hello world");
        on_disk_index.add_document("table1", "column1", 1, "goodbye world");
        on_disk_index.add_document("table1", "column2", 0, "rust programming");
        on_disk_index.add_document("table2", "column1", 0, "world peace");
    }
}
