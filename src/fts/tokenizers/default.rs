use serde::{Deserialize, Serialize};

use super::tokenizer::Tokenizer;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DefaultTokenizer;

impl Tokenizer for DefaultTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty()))
    }

    fn new() -> Self {
        DefaultTokenizer
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn tokenizer_test() {
        use super::Tokenizer;
        let tokenizer = super::DefaultTokenizer::new();
        let tokens: Vec<&str> = tokenizer.tokenize("Hello, World!").collect();
        assert_eq!(tokens, vec!["Hello", "World"]);

        let tokens: Vec<&str> = tokenizer.tokenize("Computer Science").collect();
        assert_eq!(tokens, vec!["Computer", "Science"]);

        let tokens: Vec<&str> = tokenizer.tokenize("Artificial Intelligence").collect();
        assert_eq!(tokens, vec!["Artificial", "Intelligence"]);
    }
}
