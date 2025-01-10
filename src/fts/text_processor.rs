use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use crate::sql::clauses::full_text_search::weight::TextWeight;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Token {
    pub text: String,
    pub position: usize,
    pub weight: f32,
    pub type_: TokenType,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TokenType {
    Word,
    NotWord,
    Number,
    Email,
    URL,
    Symbol,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TsVector {
    pub tokens: Vec<Token>,
    pub positions: Vec<usize>,
    pub weights: Vec<f32>,
}

impl TsVector {
    pub fn new(tokens: Vec<Token>) -> Self {
        let positions: Vec<usize> = tokens.iter().map(|t| t.position).collect();
        let weights: Vec<f32> = tokens.iter().map(|t| t.weight).collect();
        Self {
            tokens,
            positions,
            weights,
        }
    }

    pub fn set_weight(&mut self, weight: TextWeight) {
        let weight_value = weight.to_f32();
        for token in &mut self.tokens {
            token.weight = weight_value;
        }
        self.weights = self.tokens.iter().map(|t| t.weight).collect();
    }

    pub fn concatenate(&mut self, other: &TsVector) {
        let offset = self.tokens.len();
        for token in &other.tokens {
            self.tokens.push(Token {
                text: token.text.clone(),
                position: token.position + offset,
                weight: token.weight,
                type_: token.type_.clone(),
            });
        }
        self.positions = self.tokens.iter().map(|t| t.position).collect();
        self.weights = self.tokens.iter().map(|t| t.weight).collect();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessedDocument {
    pub tokens: Vec<Token>,
    pub vector: TsVector,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessedQuery {
    pub tokens: Vec<Token>,
    pub operators: Vec<QueryOperator>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryOperator {
    And,
    Or,
    Not,
    Phrase(Vec<Token>),
    Proximity(Vec<Token>, usize),
}

pub trait TextProcessor: Send + Sync {
    fn process_document(&self, text: &str, language: Option<&str>) -> ProcessedDocument;
    fn process_query(&self, query: &str, language: Option<&str>) -> ProcessedQuery;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_creation() {
        let token = Token {
            text: "test".to_string(),
            position: 1,
            weight: 1.0,
            type_: TokenType::Word,
        };
        assert_eq!(token.text, "test");
        assert_eq!(token.position, 1);
        assert_eq!(token.weight, 1.0);
        assert_eq!(token.type_, TokenType::Word);
    }

    #[test]
    fn test_ts_vector_creation() {
        let tokens = vec![
            Token {
                text: "hello".to_string(),
                position: 1,
                weight: 1.0,
                type_: TokenType::Word,
            },
            Token {
                text: "world".to_string(),
                position: 2,
                weight: 1.0,
                type_: TokenType::Word,
            },
        ];
        let vector = TsVector::new(tokens);

        assert_eq!(vector.tokens.len(), 2);
        assert_eq!(vector.positions.len(), 2);
        assert_eq!(vector.weights.len(), 2);
    }

    #[test]
    fn test_set_weight() {
        let tokens = vec![
            Token {
                text: "hello".to_string(),
                position: 1,
                weight: 1.0,
                type_: TokenType::Word,
            },
        ];
        let mut vector = TsVector::new(tokens);
        vector.set_weight(TextWeight::A);
        assert_eq!(vector.tokens[0].weight, 1.0);
        vector.set_weight(TextWeight::D);
        assert_eq!(vector.tokens[0].weight, 0.1);
    }

    #[test]
    fn test_concatenate() {
        let tokens1 = vec![
            Token {
                text: "hello".to_string(),
                position: 1,
                weight: 1.0,
                type_: TokenType::Word,
            },
        ];
        let tokens2 = vec![
            Token {
                text: "world".to_string(),
                position: 1,
                weight: 0.4,
                type_: TokenType::Word,
            },
        ];
        let mut vector1 = TsVector::new(tokens1);
        let vector2 = TsVector::new(tokens2);
        vector1.concatenate(&vector2);
        assert_eq!(vector1.tokens.len(), 2);
        assert_eq!(vector1.tokens[1].position, 2);
        assert_eq!(vector1.tokens[1].weight, 0.4);
    }
} 