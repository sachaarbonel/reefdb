use std::collections::HashSet;
use std::any::Any;
use serde::{Serialize, Deserialize};

pub mod english;
pub use self::english::EnglishProcessor;

pub trait LanguageProcessor: Send + Sync + Any {
    fn stem(&self, word: &str) -> String;
    fn is_stop_word(&self, word: &str) -> bool;
    fn get_stop_words(&self) -> &HashSet<String>;
    fn normalize(&self, text: &str) -> String;
    fn get_config(&self) -> &LanguageConfig;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageConfig {
    pub stop_words: HashSet<String>,
    pub language_code: String,
    pub enable_stemming: bool,
    pub enable_stop_words: bool,
}

impl Default for LanguageConfig {
    fn default() -> Self {
        LanguageConfig {
            stop_words: HashSet::new(),
            language_code: "en".to_string(),
            enable_stemming: true,
            enable_stop_words: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_config_default() {
        let config = LanguageConfig::default();
        assert_eq!(config.language_code, "en");
        assert!(config.enable_stemming);
        assert!(config.enable_stop_words);
        assert!(config.stop_words.is_empty());
    }
} 