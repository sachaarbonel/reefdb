#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Plain,      // plainto_tsquery
    Phrase,     // phraseto_tsquery
    WebStyle,   // websearch_to_tsquery
    Raw,        // to_tsquery
}

#[derive(Debug, Clone, PartialEq)]
pub enum Language {
    English,
    // Add more languages as needed
}

impl Default for Language {
    fn default() -> Self {
        Language::English
    }
}

#[derive(Debug)]
pub enum ParseError {
    EmptyQuery,
    UnmatchedNegation,
    InvalidOperator(String),
} 