use std::collections::HashMap;
use std::collections::HashSet;

use super::tokenizers::tokenizer::Tokenizer;

#[derive(Debug)]
pub struct InvertedIndex<T: Tokenizer> {
    index: HashMap<String, HashMap<String, HashMap<String, HashSet<usize>>>>,
    tokenizer: T,
}

impl<T: Tokenizer> InvertedIndex<T> {
    pub fn new() -> Self {
        InvertedIndex {
            index: HashMap::new(),
            tokenizer: T::new(),
        }
    }

    pub fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        self.remove_document(table, column, row_id);
        self.add_document(table, column, row_id, text);
    }

    pub fn add_column(&mut self, table: &str, column: &str) {
        let table_entry = self
            .index
            .entry(table.to_string())
            .or_insert(HashMap::new());
        table_entry.insert(column.to_string(), HashMap::new());
    }

    pub fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        let table_entry = self
            .index
            .entry(table.to_string())
            .or_insert(HashMap::new());
        let column_entry = table_entry
            .entry(column.to_string())
            .or_insert(HashMap::new());

        for word in self.tokenizer.tokenize(text) {
            let word_entry = column_entry
                .entry(word.to_string())
                .or_insert(HashSet::new());
            word_entry.insert(row_id);
        }
    }

    pub fn remove_document(&mut self, table: &str, column: &str, row_id: usize) {
        if let Some(table_entry) = self.index.get_mut(table) {
            if let Some(column_entry) = table_entry.get_mut(column) {
                for value in column_entry.values_mut() {
                    value.remove(&row_id);
                }
            }
        }
    }

    pub fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize> {
        let mut results = HashSet::new();
        if let Some(table_entry) = self.index.get(table) {
            if let Some(column_entry) = table_entry.get(column) {
                for word in self.tokenizer.tokenize(query) {
                    if let Some(word_entry) = column_entry.get(word) {
                        results.extend(word_entry);
                    }
                }
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    

    use crate::indexes::tokenizers::default::DefaultTokenizer;

    use super::*;

    #[test]
    fn test_inverted_index() {
        let mut index: InvertedIndex<DefaultTokenizer> = InvertedIndex::new();

        // Add documents
        index.add_document("table1", "column1", 0, "hello world");
        index.add_document("table1", "column1", 1, "goodbye world");
        index.add_document("table1", "column2", 0, "rust programming");
        index.add_document("table2", "column1", 0, "world peace");

        // Search
        let results = index.search("table1", "column1", "world");
        let expected: HashSet<usize> = [0, 1].iter().cloned().collect();
        assert_eq!(results, expected);

        let results = index.search("table1", "column2", "rust");
        let expected: HashSet<usize> = [0].iter().cloned().collect();
        assert_eq!(results, expected);

        let results = index.search("table2", "column1", "world");
        let expected: HashSet<usize> = [0].iter().cloned().collect();
        assert_eq!(results, expected);

        // Remove document
        index.remove_document("table1", "column1", 0);

        // Search after removing document
        let results = index.search("table1", "column1", "world");
        let expected: HashSet<usize> = [1].iter().cloned().collect();
        assert_eq!(results, expected);
    }
}
