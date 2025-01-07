use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

use super::search::Search;
use super::tokenizers::tokenizer::Tokenizer;

#[derive(Debug, Serialize, Deserialize, Clone)]
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
                .entry(word.to_lowercase())
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
        if let Some(table_entry) = self.index.get(table) {
            if let Some(column_entry) = table_entry.get(column) {
                let query_tokens: Vec<String> = self.tokenizer.tokenize(query)
                    .map(|s| s.to_lowercase())
                    .collect();
                
                if query_tokens.is_empty() {
                    return HashSet::new();
                }

                // Get results for first token
                let mut results = match column_entry.get(&query_tokens[0]) {
                    Some(word_entry) => word_entry.clone(),
                    None => return HashSet::new(),
                };

                // Intersect with results for remaining tokens
                for token in query_tokens.iter().skip(1) {
                    if let Some(word_entry) = column_entry.get(token) {
                        results.retain(|id| word_entry.contains(id));
                    } else {
                        return HashSet::new();
                    }
                }
                
                results
            } else {
                HashSet::new()
            }
        } else {
            HashSet::new()
        }
    }
}

impl<T: Tokenizer + Serialize + for<'de> Deserialize<'de>> Search for InvertedIndex<T> {
    type NewArgs = ();

    fn new(_: Self::NewArgs) -> Self {
        InvertedIndex::new()
    }

    fn add_column(&mut self, table: &str, column: &str) {
        InvertedIndex::add_column(self, table, column)
    }

    fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize> {
        InvertedIndex::search(self, table, column, query)
    }

    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        InvertedIndex::add_document(self, table, column, row_id, text)
    }

    fn remove_document(&mut self, table: &str, column: &str, row_id: usize) {
        InvertedIndex::remove_document(self, table, column, row_id)
    }

    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        InvertedIndex::update_document(self, table, column, row_id, text)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::indexes::fts::{memory::InvertedIndex, tokenizers::default::DefaultTokenizer};

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
