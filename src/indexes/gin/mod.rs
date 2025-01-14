use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::fts::text_processor::QueryOperator;
use crate::fts::text_processor::TokenType;
use crate::fts::DefaultTextProcessor;
use crate::fts::search::Search;
use crate::fts::tokenizers::tokenizer::Tokenizer;
use crate::fts::tokenizers::default::DefaultTokenizer;

mod evaluator;
use evaluator::QueryEvaluator;

/// Stores positions of a token in a document
type Positions = Vec<usize>;
/// Maps document ID to token positions
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct DocumentMap(HashMap<usize, Positions>);
/// Maps token to document positions
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct TokenMap(HashMap<String, DocumentMap>);
/// Maps column name to token information
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct ColumnMap(HashMap<String, TokenMap>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GinIndex<T: Tokenizer> {
    index: HashMap<String, ColumnMap>,
    tokenizer: T,
    text_processor: DefaultTextProcessor,
    evaluator: QueryEvaluator,
}

impl DocumentMap {
    fn insert(&mut self, doc_id: usize, position: usize) {
        self.0.entry(doc_id)
            .or_insert_with(Vec::new)
            .push(position);
    }

    fn get(&self, doc_id: usize) -> Option<&Positions> {
        self.0.get(&doc_id)
    }

    fn remove(&mut self, doc_id: usize) {
        self.0.remove(&doc_id);
    }

    fn doc_ids(&self) -> HashSet<usize> {
        self.0.keys().cloned().collect()
    }
}

impl TokenMap {
    fn entry(&mut self, token: String) -> std::collections::hash_map::Entry<String, DocumentMap> {
        self.0.entry(token)
    }

    fn get(&self, token: &str) -> Option<&DocumentMap> {
        self.0.get(token)
    }

    fn values_mut(&mut self) -> std::collections::hash_map::ValuesMut<String, DocumentMap> {
        self.0.values_mut()
    }
}

impl ColumnMap {
    fn entry(&mut self, column: String) -> std::collections::hash_map::Entry<String, TokenMap> {
        self.0.entry(column)
    }

    fn get(&self, column: &str) -> Option<&TokenMap> {
        self.0.get(column)
    }

    fn get_mut(&mut self, column: &str) -> Option<&mut TokenMap> {
        self.0.get_mut(column)
    }
}

impl<T: Tokenizer> GinIndex<T> {
    pub fn new() -> Self {
        GinIndex {
            index: HashMap::new(),
            tokenizer: T::new(),
            text_processor: DefaultTextProcessor::new(),
            evaluator: QueryEvaluator::new(),
        }
    }

    pub fn add_column(&mut self, table: &str, column: &str) {
        self.index.entry(table.to_string())
            .or_insert_with(ColumnMap::default)
            .0.entry(column.to_string())
            .or_insert_with(TokenMap::default);
    }

    // Add a method to directly insert raw bytes as a token (for testing purposes)
    #[cfg(test)]
    pub fn add_raw_token(&mut self, raw_bytes: &[u8], row_id: usize) {
        let table = "test_table";
        let column = "test_column";
        
        // Ensure the table and column exist
        self.add_column(table, column);
        
        // Get the column map
        if let Some(column_map) = self.index.get_mut(table) {
            // Get the token map
            if let Some(token_map) = column_map.0.get_mut(column) {
                // Create a document map for this token
                let doc_map = token_map.0
                    .entry(unsafe { String::from_utf8_unchecked(raw_bytes.to_vec()) })
                    .or_insert_with(DocumentMap::default);
                
                // Add the document ID with position 0
                doc_map.insert(row_id, 0);
            }
        }
    }

    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        let table_entry = self.index
            .entry(table.to_string())
            .or_insert_with(ColumnMap::default);
        
        let column_entry = table_entry
            .entry(column.to_string())
            .or_insert_with(TokenMap::default);

        let processed = self.text_processor.process_document(text, Some("english"));
        for token in processed.tokens {
            column_entry
                .entry(token.text)
                .or_insert_with(DocumentMap::default)
                .0
                .entry(row_id)
                .or_insert_with(Vec::new)
                .push(token.position);
        }
    }

    fn remove_document(&mut self, table: &str, column: &str, row_id: usize) {
        if let Some(table_entry) = self.index.get_mut(table) {
            if let Some(token_map) = table_entry.0.get_mut(column) {
                for doc_map in token_map.0.values_mut() {
                    doc_map.remove(row_id);
                }
            }
        }
    }

    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        self.remove_document(table, column, row_id);
        self.add_document(table, column, row_id, text);
    }

    pub fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize> {
        if let Some(table_entry) = self.index.get(table) {
            if let Some(column_entry) = table_entry.get(column) {
                self.evaluator.evaluate(column_entry, query)
            } else {
                HashSet::new()
            }
        } else {
            HashSet::new()
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, HashSet<usize>)> + '_ {
        self.index
            .values()
            .flat_map(|column_map| column_map.0.values())
            .flat_map(|token_map| token_map.0.iter())
            .map(|(token, doc_map)| (token.as_bytes().to_vec(), doc_map.doc_ids()))
    }
}

impl<T: Tokenizer + Serialize + for<'de> Deserialize<'de>> Search for GinIndex<T> {
    type NewArgs = ();

    fn new(_: Self::NewArgs) -> Self {
        GinIndex::new()
    }

    fn add_column(&mut self, table: &str, column: &str) {
        GinIndex::add_column(self, table, column)
    }

    fn search(&self, table: &str, column: &str, query: &str) -> HashSet<usize> {
        GinIndex::search(self, table, column, query)
    }

    fn add_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        GinIndex::add_document(self, table, column, row_id, text)
    }

    fn remove_document(&mut self, table: &str, column: &str, row_id: usize) {
        GinIndex::remove_document(self, table, column, row_id)
    }

    fn update_document(&mut self, table: &str, column: &str, row_id: usize, text: &str) {
        GinIndex::update_document(self, table, column, row_id, text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::tokenizers::default::DefaultTokenizer;

    #[test]
    fn test_gin_index() {
        let mut index: GinIndex<DefaultTokenizer> = GinIndex::new();

        // Add documents
        index.add_document("table1", "column1", 0, "hello world");
        index.add_document("table1", "column1", 1, "goodbye world");
        index.add_document("table1", "column2", 0, "rust programming");
        index.add_document("table2", "column1", 0, "world peace");

        // Test basic search
        let results = index.search("table1", "column1", "world");
        println!("Basic search results for 'world': {:?}", results);
        let expected: HashSet<usize> = [0, 1].iter().cloned().collect();
        assert_eq!(results, expected);

        // Test phrase search
        let results = index.search("table1", "column1", "\"hello world\"");
        println!("Phrase search results for 'hello world': {:?}", results);
        let expected: HashSet<usize> = [0].iter().cloned().collect();
        assert_eq!(results, expected);

        // Test AND operation
        let results = index.search("table1", "column1", "hello AND world");
        println!("AND operation results for 'hello AND world': {:?}", results);
        let expected: HashSet<usize> = [0].iter().cloned().collect();
        assert_eq!(results, expected);

        // Test OR operation
        let results = index.search("table1", "column1", "hello OR goodbye");
        println!("OR operation results for 'hello OR goodbye': {:?}", results);
        let expected: HashSet<usize> = [0, 1].iter().cloned().collect();
        assert_eq!(results, expected);

        // Test document removal
        index.remove_document("table1", "column1", 0);
        let results = index.search("table1", "column1", "hello");
        assert!(results.is_empty());
    }
} 