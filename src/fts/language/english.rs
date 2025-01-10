use std::collections::HashSet;
use std::fmt;
use std::any::Any;
use rust_stemmers::{Algorithm, Stemmer};
use super::{LanguageProcessor, LanguageConfig};
use lazy_static::lazy_static;

lazy_static! {
    static ref ENGLISH_STOP_WORDS: HashSet<String> = {
        vec![
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
            "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
            "to", "was", "were", "will", "with"
        ].into_iter().map(String::from).collect()
    };
}

pub struct EnglishProcessor {
    config: LanguageConfig,
    stemmer: Stemmer,
}

impl fmt::Debug for EnglishProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnglishProcessor")
            .field("config", &self.config)
            .field("stemmer", &"<stemmer>")
            .finish()
    }
}

impl EnglishProcessor {
    pub fn new(config: Option<LanguageConfig>) -> Self {
        let mut config = config.unwrap_or_default();
        if config.stop_words.is_empty() {
            config.stop_words = ENGLISH_STOP_WORDS.clone();
        }
        
        EnglishProcessor {
            config,
            stemmer: Stemmer::create(Algorithm::English),
        }
    }
}

impl LanguageProcessor for EnglishProcessor {
    fn stem(&self, word: &str) -> String {
        if self.config.enable_stemming {
            self.stemmer.stem(word).to_string()
        } else {
            word.to_string()
        }
    }

    fn is_stop_word(&self, word: &str) -> bool {
        if self.config.enable_stop_words {
            self.config.stop_words.contains(&word.to_lowercase())
        } else {
            false
        }
    }

    fn get_stop_words(&self) -> &HashSet<String> {
        &self.config.stop_words
    }

    fn normalize(&self, text: &str) -> String {
        // Convert to lowercase
        let text = text.to_lowercase();
        
        // Replace punctuation with spaces
        let text: String = text.chars()
            .map(|c| if c.is_alphanumeric() { c } else { ' ' })
            .collect();
        
        // Collapse multiple whitespace into single space and trim
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn get_config(&self) -> &LanguageConfig {
        &self.config
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_english_processor() {
        let processor = EnglishProcessor::new(None);
        
        // Test stemming
        assert_eq!(processor.stem("running"), "run");
        assert_eq!(processor.stem("books"), "book");
        
        // Test stop words
        assert!(processor.is_stop_word("the"));
        assert!(processor.is_stop_word("and"));
        assert!(!processor.is_stop_word("book"));
        
        // Test normalization
        assert_eq!(processor.normalize("Hello, World!"), "hello world");
        assert_eq!(processor.normalize("Running-Fast"), "running fast");
    }

    #[test]
    fn test_custom_config() {
        let mut config = LanguageConfig::default();
        config.enable_stemming = false;
        let processor = EnglishProcessor::new(Some(config));
        
        // Test stemming disabled
        assert_eq!(processor.stem("running"), "running");
        assert_eq!(processor.stem("books"), "books");
    }
} 