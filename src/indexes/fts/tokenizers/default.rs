use serde::{Deserialize, Serialize};

use super::tokenizer::Tokenizer;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DefaultTokenizer;

impl Tokenizer for DefaultTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(text.split_whitespace())
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
        let tokens: Vec<&str> = tokenizer.tokenize("Hello World").collect();
        assert_eq!(tokens, vec!["Hello", "World"]);
    }
}
