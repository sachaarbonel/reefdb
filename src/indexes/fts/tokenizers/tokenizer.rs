pub trait Tokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Box<dyn Iterator<Item = &'a str> + 'a>;
    fn new() -> Self;
}
