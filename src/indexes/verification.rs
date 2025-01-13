use std::collections::{HashMap, HashSet};
use crate::error::ReefDBError;
use crate::indexes::index_manager::{IndexType, IndexManager};
use crate::fts::tokenizers::tokenizer::Tokenizer;

pub trait IndexVerification {
    fn verify_index_consistency(&self, table: &str, column: &str) -> Result<VerificationResult, ReefDBError>;
}

#[derive(Debug)]
pub struct VerificationResult {
    pub is_consistent: bool,
    pub issues: Vec<VerificationIssue>,
}

#[derive(Debug)]
pub enum VerificationIssue {
    DuplicateRowId {
        value: Vec<u8>,
        row_id: usize,
    },
    OrphanedRowId {
        value: Vec<u8>,
        row_id: usize,
    },
    InvalidValue {
        value: Vec<u8>,
        reason: String,
    },
}

impl<T> IndexVerification for T where T: IndexManager {
    fn verify_index_consistency(&self, table: &str, column: &str) -> Result<VerificationResult, ReefDBError> {
        let index = self.get_index(table, column)?;
        
        match index {
            IndexType::BTree(btree) => verify_btree_consistency(btree),
            IndexType::GIN(gin) => verify_gin_consistency(gin),
        }
    }
}

fn verify_btree_consistency(index: &crate::indexes::btree::BTreeIndex) -> Result<VerificationResult, ReefDBError> {
    let mut issues = Vec::new();
    let mut seen_row_ids = HashSet::new();

    // Check for duplicate row IDs across different values
    for (value, row_ids) in index.iter() {
        for row_id in row_ids {
            if !seen_row_ids.insert(*row_id) {
                issues.push(VerificationIssue::DuplicateRowId {
                    value: value.clone(),
                    row_id: *row_id,
                });
            }
        }
    }

    Ok(VerificationResult {
        is_consistent: issues.is_empty(),
        issues,
    })
}

fn verify_gin_consistency(index: &crate::indexes::gin::GinIndex<crate::fts::tokenizers::default::DefaultTokenizer>) -> Result<VerificationResult, ReefDBError> {
    let mut issues = Vec::new();

    // Check for invalid UTF-8 tokens
    for (token, _) in index.iter() {
        if let Err(e) = String::from_utf8(token.clone()) {
            println!("Found invalid UTF-8 token: {:?}, error: {}", token, e);
            issues.push(VerificationIssue::InvalidValue {
                value: token.clone(),
                reason: format!("Invalid UTF-8 token: {}", e),
            });
        }
    }

    Ok(VerificationResult {
        is_consistent: issues.is_empty(),
        issues,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexes::btree::BTreeIndex;
    use crate::indexes::gin::GinIndex;
    use crate::indexes::DefaultIndexManager;
    use crate::fts::tokenizers::default::DefaultTokenizer;
    use crate::storage::memory::InMemoryStorage;
    use crate::fts::search::Search;

    #[test]
    fn test_btree_index_verification() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add some valid entries
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 2);
        
        manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
        
        let result = IndexVerification::verify_index_consistency(&manager, "test_table", "test_column").unwrap();
        assert!(result.is_consistent);
        assert!(result.issues.is_empty());
    }

    #[test]
    fn test_btree_index_duplicate_row_ids() {
        let mut manager = DefaultIndexManager::new();
        let mut btree = BTreeIndex::new();
        
        // Add entries with duplicate row IDs
        btree.add_entry(vec![1, 2, 3], 1);
        btree.add_entry(vec![4, 5, 6], 1);  // Same row_id
        
        manager.create_index("test_table", "test_column", IndexType::BTree(btree)).unwrap();
        
        let result = IndexVerification::verify_index_consistency(&manager, "test_table", "test_column").unwrap();
        assert!(!result.is_consistent);
        assert_eq!(result.issues.len(), 1);
        
        match &result.issues[0] {
            VerificationIssue::DuplicateRowId { row_id, .. } => {
                assert_eq!(*row_id, 1);
            }
            _ => panic!("Expected DuplicateRowId issue"),
        }
    }

    #[test]
    fn test_gin_index_verification() {
        let mut manager = DefaultIndexManager::new();
        let mut gin = GinIndex::<DefaultTokenizer>::new();
        
        println!("Adding documents to GIN index...");
        // Add some valid entries
        gin.add_document("test_table", "test_column", 1, "hello world");
        gin.add_document("test_table", "test_column", 2, "testing gin");
        
        println!("Creating index in manager...");
        manager.create_index("test_table", "test_column", IndexType::GIN(gin)).unwrap();
        
        println!("Verifying index consistency...");
        let result = IndexVerification::verify_index_consistency(&manager, "test_table", "test_column").unwrap();
        println!("Verification result: {:?}", result);
        
        assert!(result.is_consistent, "Index should be consistent, but found issues: {:?}", result.issues);
        assert!(result.issues.is_empty(), "Expected no issues, but found: {:?}", result.issues);
    }

    #[test]
    fn test_gin_index_invalid_utf8() {
        let mut manager = DefaultIndexManager::new();
        let mut gin = GinIndex::<DefaultTokenizer>::new();
        
        println!("Adding valid document...");
        gin.add_document("test_table", "test_column", 1, "hello world");
        
        println!("Creating invalid UTF-8 sequence...");
        // Create an invalid UTF-8 sequence that will definitely fail
        let invalid_utf8 = vec![0xFF, 0xFF];
        println!("Invalid UTF-8 bytes: {:?}", invalid_utf8);
        
        // Add the invalid UTF-8 bytes directly to the index's internal storage
        // This simulates corrupted data in the index
        gin.add_raw_token(&invalid_utf8, 2);
        
        println!("Creating index...");
        manager.create_index("test_table", "test_column", IndexType::GIN(gin)).unwrap();
        
        println!("Verifying index consistency...");
        let result = IndexVerification::verify_index_consistency(&manager, "test_table", "test_column").unwrap();
        println!("Verification result: {:?}", result);
        
        assert!(!result.is_consistent, "Expected index to be inconsistent");
        
        let has_invalid_value = result.issues.iter().any(|issue| {
            let is_invalid = matches!(issue, VerificationIssue::InvalidValue { .. });
            println!("Issue: {:?}, is invalid value: {}", issue, is_invalid);
            is_invalid
        });
        assert!(has_invalid_value, "Expected to find an InvalidValue issue");
    }
} 