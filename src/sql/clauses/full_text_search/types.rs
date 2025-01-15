#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Plain,      // plainto_tsquery
    Phrase,     // phraseto_tsquery
    WebStyle,   // websearch_to_tsquery
    Raw,        // to_tsquery
}

#[derive(Debug)]
pub enum ParseError {
    EmptyQuery,
    UnmatchedNegation,
    InvalidOperator(String),
    InvalidSyntax(String),
} 