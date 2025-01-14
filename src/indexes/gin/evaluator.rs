use std::collections::{HashMap, HashSet};
use crate::fts::text_processor::{QueryOperator, TokenType};
use crate::fts::DefaultTextProcessor;
use serde::{Serialize, Deserialize};

use super::{TokenMap, DocumentMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEvaluator {
    text_processor: DefaultTextProcessor,
}

impl QueryEvaluator {
    pub fn new() -> Self {
        Self {
            text_processor: DefaultTextProcessor::new(),
        }
    }

    pub fn evaluate(&self, column_entry: &TokenMap, query: &str) -> HashSet<usize> {
        let processed = self.text_processor.process_query(query, Some("english"));
        println!("Processed query tokens: {:?}", processed.tokens);
        println!("Processed query operators: {:?}", processed.operators);
        
        if processed.tokens.is_empty() {
            return HashSet::new();
        }

        // Convert processed tokens and operators into the format expected by the evaluator
        let mut token_ops: Vec<(String, QueryOperator)> = Vec::new();
        for (i, token) in processed.tokens.iter().enumerate() {
            let op = if token.type_ == TokenType::NotWord {
                QueryOperator::Not
            } else if i > 0 && i - 1 < processed.operators.len() {
                processed.operators[i - 1].clone()
            } else {
                QueryOperator::And // Default to AND if no operator specified
            };
            token_ops.push((token.text.to_lowercase(), op));
        }

        self.evaluate_tokens(column_entry, token_ops)
    }

    fn evaluate_tokens(&self, column_entry: &TokenMap, processed_tokens: Vec<(String, QueryOperator)>) -> HashSet<usize> {
        if processed_tokens.is_empty() {
            return HashSet::new();
        }

        // If there's only one token and no operators, return all documents containing that token
        if processed_tokens.len() == 1 && matches!(processed_tokens[0].1, QueryOperator::And) {
            let token_text = &processed_tokens[0].0;
            println!("Single token search for: {}", token_text);
            return match column_entry.get(token_text) {
                Some(doc_map) => doc_map.doc_ids(),
                None => HashSet::new(),
            };
        }

        let mut current_set: Option<HashSet<usize>> = None;

        // Get all document IDs in the column for NOT operations
        let mut all_docs = HashSet::new();
        for doc_map in column_entry.0.values() {
            all_docs.extend(doc_map.doc_ids());
        }

        for (i, (token_text, operator)) in processed_tokens.iter().enumerate() {
            println!("Processing token {}: {}", i, token_text);
            let token_results = match column_entry.get(token_text) {
                Some(doc_map) => {
                    let results = doc_map.doc_ids();
                    println!("Documents containing '{}': {:?}", token_text, results);
                    results
                },
                None => HashSet::new(),
            };

            if current_set.is_none() {
                current_set = Some(token_results);
                println!("Initial result set: {:?}", current_set);
                continue;
            }

            let mut new_set = HashSet::new();
            match operator {
                QueryOperator::And => {
                    for id in current_set.as_ref().unwrap() {
                        if token_results.contains(id) {
                            new_set.insert(*id);
                        }
                    }
                    println!("After AND operation: {:?}", new_set);
                }
                QueryOperator::Or => {
                    new_set.extend(current_set.as_ref().unwrap());
                    new_set.extend(token_results);
                    println!("After OR operation: {:?}", new_set);
                }
                QueryOperator::Not => {
                    // For NOT operation, we want documents that are in current_set but NOT in token_results
                    for id in current_set.as_ref().unwrap() {
                        if !token_results.contains(id) {
                            new_set.insert(*id);
                        }
                    }
                    println!("After NOT operation: {:?}", new_set);
                }
                QueryOperator::Phrase(ref tokens) => {
                    let token_strings: Vec<String> = tokens.iter().map(|t| t.text.to_lowercase()).collect();
                    println!("Checking phrase: {:?}", token_strings);
                    for id in current_set.as_ref().unwrap() {
                        if Self::check_phrase(column_entry, *id, &token_strings) {
                            new_set.insert(*id);
                        }
                    }
                    println!("After phrase check: {:?}", new_set);
                }
                QueryOperator::Proximity(ref tokens, distance) => {
                    let token_strings: Vec<String> = tokens.iter().map(|t| t.text.to_lowercase()).collect();
                    println!("Checking proximity for tokens: {:?} with distance {}", token_strings, distance);
                    for id in current_set.as_ref().unwrap() {
                        if Self::check_proximity(column_entry, *id, &token_strings, *distance) {
                            new_set.insert(*id);
                        }
                    }
                    println!("After proximity check: {:?}", new_set);
                }
            }
            current_set = Some(new_set);
        }

        current_set.unwrap_or_default()
    }

    fn check_phrase(column_entry: &TokenMap, doc_id: usize, tokens: &[String]) -> bool {
        if tokens.is_empty() {
            return true;
        }

        let first_positions = match column_entry.get(&tokens[0]) {
            Some(doc_map) => match doc_map.get(doc_id) {
                Some(positions) => positions,
                None => return false,
            },
            None => return false,
        };

        'outer: for &start_pos in first_positions {
            for (i, token) in tokens.iter().skip(1).enumerate() {
                let expected_pos = start_pos + i + 1;
                match column_entry.get(token) {
                    Some(doc_map) => match doc_map.get(doc_id) {
                        Some(positions) => {
                            if !positions.contains(&expected_pos) {
                                continue 'outer;
                            }
                        }
                        None => return false,
                    },
                    None => return false,
                }
            }
            return true;
        }
        false
    }

    fn check_proximity(column_entry: &TokenMap, doc_id: usize, tokens: &[String], max_distance: usize) -> bool {
        if tokens.len() < 2 {
            return true;
        }

        let mut all_positions: Vec<Vec<usize>> = Vec::new();
        for token in tokens {
            match column_entry.get(token) {
                Some(doc_map) => match doc_map.get(doc_id) {
                    Some(positions) => all_positions.push(positions.clone()),
                    None => return false,
                },
                None => return false,
            }
        }

        for &pos1 in &all_positions[0] {
            for positions in all_positions.iter().skip(1) {
                let mut found = false;
                for &pos2 in positions {
                    if pos1.abs_diff(pos2) <= max_distance {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_index() -> TokenMap {
        let mut token_map = TokenMap::default();
        
        // Doc 1: "rust programming"
        let mut doc1_map = DocumentMap::default();
        doc1_map.0.insert(1, vec![0]); // "rust" at position 0
        token_map.0.insert("rust".to_string(), doc1_map);
        
        let mut doc1_map = DocumentMap::default();
        doc1_map.0.insert(1, vec![1]); // "programming" at position 1
        token_map.0.insert("program".to_string(), doc1_map);

        // Doc 2: "rust web development"
        let mut doc2_map = DocumentMap::default();
        doc2_map.0.insert(2, vec![0]); // "rust" at position 0
        token_map.0.get_mut("rust").unwrap().0.insert(2, vec![0]);
        
        let mut doc2_map = DocumentMap::default();
        doc2_map.0.insert(2, vec![1]); // "web" at position 1
        token_map.0.insert("web".to_string(), doc2_map);
        
        let mut doc2_map = DocumentMap::default();
        doc2_map.0.insert(2, vec![2]); // "development" at position 2
        token_map.0.insert("develop".to_string(), doc2_map);

        // Doc 3: "database systems"
        let mut doc3_map = DocumentMap::default();
        doc3_map.0.insert(3, vec![0]); // "database" at position 0
        token_map.0.insert("databas".to_string(), doc3_map);
        
        let mut doc3_map = DocumentMap::default();
        doc3_map.0.insert(3, vec![1]); // "systems" at position 1
        token_map.0.insert("system".to_string(), doc3_map);

        token_map
    }

    #[test]
    fn test_single_token_search() {
        let evaluator = QueryEvaluator::new();
        let token_map = setup_test_index();

        let results = evaluator.evaluate(&token_map, "rust");
        assert_eq!(results, [1, 2].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "database");
        assert_eq!(results, [3].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "nonexistent");
        assert!(results.is_empty());
    }

    #[test]
    fn test_and_operation() {
        let evaluator = QueryEvaluator::new();
        let token_map = setup_test_index();

        let results = evaluator.evaluate(&token_map, "rust web");
        assert_eq!(results, [2].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "rust programming");
        assert_eq!(results, [1].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "rust database");
        assert!(results.is_empty());
    }

    #[test]
    fn test_or_operation() {
        let evaluator = QueryEvaluator::new();
        let token_map = setup_test_index();

        let results = evaluator.evaluate(&token_map, "rust OR database");
        assert_eq!(results, [1, 2, 3].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "web OR programming");
        assert_eq!(results, [1, 2].iter().cloned().collect::<HashSet<_>>());
    }

    #[test]
    fn test_not_operation() {
        let evaluator = QueryEvaluator::new();
        let token_map = setup_test_index();

        let results = evaluator.evaluate(&token_map, "rust !database");
        assert_eq!(results, [1, 2].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "rust !web");
        assert_eq!(results, [1].iter().cloned().collect::<HashSet<_>>());
    }

    #[test]
    fn test_phrase_search() {
        let evaluator = QueryEvaluator::new();
        let token_map = setup_test_index();

        let results = evaluator.evaluate(&token_map, "\"rust programming\"");
        assert_eq!(results, [1].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "\"rust web\"");
        assert_eq!(results, [2].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "\"web rust\"");
        assert!(results.is_empty());
    }

    #[ignore]
    fn test_complex_queries() {
        let evaluator = QueryEvaluator::new();
        let token_map = setup_test_index();

        let results = evaluator.evaluate(&token_map, "rust web !database");
        assert_eq!(results, [2].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "\"rust web\" OR programming");
        assert_eq!(results, [1, 2].iter().cloned().collect::<HashSet<_>>());

        let results = evaluator.evaluate(&token_map, "rust !web !database");
        assert_eq!(results, [1].iter().cloned().collect::<HashSet<_>>());
    }
}
