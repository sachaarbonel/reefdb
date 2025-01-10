use std::collections::HashMap;
use std::fmt;
use std::any::Any;
use super::{
    text_processor::{ProcessedQuery, Token, TokenType, TsVector, QueryOperator},
    language::{LanguageProcessor, english::EnglishProcessor, LanguageConfig},
};
use serde::{Serialize, Deserialize};
use crate::sql::clauses::full_text_search::query::{TSQuery, QueryOperator as SqlQueryOperator, QueryType};
use super::text_processor::QueryOperator as FtsQueryOperator;

#[derive(Serialize, Deserialize)]
struct SerializedProcessor {
    language_configs: HashMap<String, LanguageConfig>,
    default_language: String,
}

pub struct DefaultTextProcessor {
    language_processors: HashMap<String, Box<dyn LanguageProcessor>>,
    default_language: String,
}

impl Serialize for DefaultTextProcessor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut language_configs = HashMap::new();
        for (lang, processor) in &self.language_processors {
            language_configs.insert(lang.clone(), processor.get_config().clone());
        }

        let serialized = SerializedProcessor {
            language_configs,
            default_language: self.default_language.clone(),
        };
        serialized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DefaultTextProcessor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedProcessor::deserialize(deserializer)?;
        let mut language_processors = HashMap::new();
        
        for (lang, config) in serialized.language_configs {
            language_processors.insert(lang, Box::new(EnglishProcessor::new(Some(config))) as Box<dyn LanguageProcessor>);
        }

        Ok(DefaultTextProcessor {
            language_processors,
            default_language: serialized.default_language,
        })
    }
}

impl fmt::Debug for DefaultTextProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultTextProcessor")
            .field("default_language", &self.default_language)
            .field("language_processors", &format!("<{} processors>", self.language_processors.len()))
            .finish()
    }
}

impl DefaultTextProcessor {
    pub fn new() -> Self {
        let mut language_processors: HashMap<String, Box<dyn LanguageProcessor>> = HashMap::new();
        language_processors.insert("english".to_string(), Box::new(EnglishProcessor::new(None)) as Box<dyn LanguageProcessor>);
        DefaultTextProcessor {
            language_processors,
            default_language: "english".to_string(),
        }
    }

    pub fn get_language_processor(&self, language: Option<&str>) -> &dyn LanguageProcessor {
        let language = language.unwrap_or(&self.default_language);
        self.language_processors
            .get(language)
            .unwrap_or_else(|| self.language_processors.get(&self.default_language).unwrap())
            .as_ref()
    }

    pub fn process_document(&self, text: &str, language: Option<&str>) -> TsVector {
        let processor = self.get_language_processor(language);
        
        // Use the language processor's normalize method
        let normalized = processor.normalize(text);
        
        let mut tokens = Vec::new();
        let mut position = 0;

        for word in normalized.split_whitespace() {
            if !processor.is_stop_word(word) {
                let stemmed = processor.stem(word);
                position += 1;
                tokens.push(Token {
                    text: stemmed,
                    position,
                    weight: 1.0,
                    type_: TokenType::Word,
                });
            }
        }

        let positions: Vec<_> = tokens.iter().map(|t| t.position).collect();
        let weights: Vec<_> = tokens.iter().map(|t| t.weight).collect();

        TsVector {
            tokens,
            positions,
            weights,
        }
    }

    pub fn process_query(&self, text: &str, language: Option<&str>) -> ProcessedQuery {
        let processor = self.get_language_processor(language);
        let query = if text.starts_with('"') && text.ends_with('"') {
            TSQuery::new(text[1..text.len()-1].to_string()).with_type(QueryType::Phrase)
        } else {
            TSQuery::new(text.to_string())
        };
        let parsed = query.parse();
        
        let mut tokens = Vec::new();
        let mut operators = Vec::new();
        let mut position = 0;

        // Handle single word queries
        if parsed.terms.is_empty() && !text.is_empty() {
            let words: Vec<&str> = text.split_whitespace().collect();
            for word in words {
                let word = word.to_lowercase();
                if !processor.is_stop_word(&word) {
                    position += 1;
                    tokens.push(Token {
                        text: processor.stem(&word),
                        position,
                        weight: 1.0,
                        type_: TokenType::Word,
                    });
                }
            }
        } else {
            for term in parsed.terms {
                let word = term.text.to_lowercase();
                if !processor.is_stop_word(&word) {
                    position += 1;
                    tokens.push(Token {
                        text: processor.stem(&word),
                        position,
                        weight: 1.0,
                        type_: if term.is_negated { TokenType::NotWord } else { TokenType::Word },
                    });
                }
            }
        }

        // If it's a phrase query, add a single Phrase operator
        if matches!(query.query_type, QueryType::Phrase) {
            operators = vec![QueryOperator::Phrase(tokens.clone())];
        } else {
            // Convert SQL operators to FTS operators
            for op in parsed.operators {
                match op {
                    SqlQueryOperator::And => operators.push(QueryOperator::And),
                    SqlQueryOperator::Or => operators.push(QueryOperator::Or),
                    SqlQueryOperator::Not => {
                        // NOT is handled via TokenType::NotWord
                        // If there are tokens before this NOT, add an AND operator
                        if !tokens.is_empty() {
                            operators.push(QueryOperator::And);
                        }
                    }
                }
            }

            // If no operators were found between tokens, default to AND
            if operators.is_empty() && tokens.len() > 1 {
                operators = vec![QueryOperator::And; tokens.len() - 1];
            }
        }

        ProcessedQuery { tokens, operators }
    }
}

impl Clone for DefaultTextProcessor {
    fn clone(&self) -> Self {
        let mut language_processors: HashMap<String, Box<dyn LanguageProcessor>> = HashMap::new();
        language_processors.insert("english".to_string(), Box::new(EnglishProcessor::new(None)) as Box<dyn LanguageProcessor>);
        DefaultTextProcessor {
            language_processors,
            default_language: self.default_language.clone(),
        }
    }
}

impl Default for DefaultTextProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_document() {
        let processor = DefaultTextProcessor::new();
        let doc = processor.process_document("Running quickly and efficiently", None);
        
        // Check stemming and stop word removal
        assert!(doc.tokens.iter().any(|t| t.text == "run"));
        assert!(doc.tokens.iter().any(|t| t.text == "quick"));
        assert!(doc.tokens.iter().any(|t| t.text == "effici")); // Porter stemmer reduces "efficiently" to "effici"
        
        // "and" should be removed as stop word
        assert!(!doc.tokens.iter().any(|t| t.text == "and"));
    }

    #[test]
    fn test_process_query() {
        let processor = DefaultTextProcessor::new();
        let query = processor.process_query("running AND quick", None);
        
        assert!(query.tokens.iter().any(|t| t.text == "run"));
        assert!(query.tokens.iter().any(|t| t.text == "quick"));
        assert_eq!(query.operators.len(), 1);
    }

    #[test]
    fn test_word_boundaries() {
        let processor = DefaultTextProcessor::new();
        let doc = processor.process_document("word1,word2;word3.word4!word5", None);
        
        // Check that words are properly separated despite punctuation
        assert!(doc.tokens.iter().any(|t| t.text == "word1"));
        assert!(doc.tokens.iter().any(|t| t.text == "word2"));
        assert!(doc.tokens.iter().any(|t| t.text == "word3"));
        assert!(doc.tokens.iter().any(|t| t.text == "word4"));
        assert!(doc.tokens.iter().any(|t| t.text == "word5"));
        
        // Check positions are sequential
        let positions: Vec<_> = doc.tokens.iter()
            .map(|t| t.position)
            .collect();
        assert_eq!(positions, vec![1, 2, 3, 4, 5]);
    }
} 