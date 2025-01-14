use super::types::{Language, QueryType, ParseError};
use super::operator::QueryOperator;
use super::term::ParsedTerm;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTSQuery {
    pub terms: Vec<ParsedTerm>,
    pub operators: Vec<QueryOperator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TSQuery {
    pub text: String,
    pub query_type: QueryType,
    pub language: Option<Language>,
}

impl TSQuery {
    pub fn new(text: String) -> Self {
        Self {
            text,
            query_type: QueryType::Plain,
            language: None,
        }
    }

    pub fn with_type(mut self, query_type: QueryType) -> Self {
        self.query_type = query_type;
        self
    }

    pub fn with_language(mut self, language: Language) -> Self {
        self.language = Some(language);
        self
    }

    pub fn function_name(&self) -> &'static str {
        match self.query_type {
            QueryType::Plain => "plainto_tsquery",
            QueryType::Phrase => "phraseto_tsquery",
            QueryType::WebStyle => "websearch_to_tsquery",
            QueryType::Raw => "to_tsquery",
        }
    }

    fn handle_special_char(&self, c: char, current_term: &mut String, terms: &mut Vec<ParsedTerm>, operators: &mut Vec<QueryOperator>, is_negated: &mut bool) {
        match c {
            '&' => {
                if let Some(term) = ParsedTerm::parse(current_term, *is_negated) {
                    terms.push(term);
                }
                operators.push(QueryOperator::And);
                current_term.clear();
                *is_negated = false;
            }
            '|' => {
                if let Some(term) = ParsedTerm::parse(current_term, *is_negated) {
                    terms.push(term);
                }
                operators.push(QueryOperator::Or);
                current_term.clear();
                *is_negated = false;
            }
            '!' => {
                *is_negated = true;
            }
            ' ' => {
                if let Some(term) = ParsedTerm::parse(current_term, *is_negated) {
                    terms.push(term);
                } else if let Some(op) = QueryOperator::from_str(current_term) {
                    operators.push(op);
                }
                current_term.clear();
                *is_negated = false;
            }
            _ => {
                current_term.push(c);
            }
        }
    }

    pub fn parse(&self) -> ParsedTSQuery {
        let mut terms = Vec::new();
        let mut operators = Vec::new();
        let mut current_term = String::new();
        let mut is_negated = false;

        // Handle empty query
        if self.text.trim().is_empty() {
            return ParsedTSQuery { terms: vec![], operators: vec![] };
        }

        // Process each character
        for c in self.text.chars() {
            self.handle_special_char(c, &mut current_term, &mut terms, &mut operators, &mut is_negated);
        }

        // Handle the last term if any
        if !current_term.is_empty() {
            if let Some(term) = ParsedTerm::parse(&current_term, is_negated) {
                terms.push(term);
            } else if let Some(op) = QueryOperator::from_str(&current_term) {
                operators.push(op);
            }
        }

        ParsedTSQuery { terms, operators }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_query() {
        let query = TSQuery::new("test query".to_string());
        assert_eq!(query.text, "test query");
        assert_eq!(query.query_type, QueryType::Plain);
        assert_eq!(query.language, None);
    }

    #[test]
    fn test_with_type() {
        let query = TSQuery::new("test".to_string())
            .with_type(QueryType::Phrase);
        assert_eq!(query.query_type, QueryType::Phrase);
    }

    #[test]
    fn test_with_language() {
        let query = TSQuery::new("test".to_string())
            .with_language(Language::English);
        assert_eq!(query.language, Some(Language::English));
    }

    #[test]
    fn test_function_name() {
        let plain = TSQuery::new("test".to_string());
        assert_eq!(plain.function_name(), "plainto_tsquery");

        let phrase = TSQuery::new("test".to_string())
            .with_type(QueryType::Phrase);
        assert_eq!(phrase.function_name(), "phraseto_tsquery");

        let web = TSQuery::new("test".to_string())
            .with_type(QueryType::WebStyle); 
        assert_eq!(web.function_name(), "websearch_to_tsquery");

        let raw = TSQuery::new("test".to_string())
            .with_type(QueryType::Raw);
        assert_eq!(raw.function_name(), "to_tsquery");
    }

    #[test]
    fn test_parse_query() {
        let query = TSQuery::new("hello & world | !database".to_string());
        let parsed = query.parse();
        assert_eq!(parsed.terms.len(), 3);
        assert_eq!(parsed.operators.len(), 2);
        assert_eq!(parsed.terms[0].text, "hello");
        assert!(!parsed.terms[0].is_negated);
        assert_eq!(parsed.terms[1].text, "world");
        assert!(!parsed.terms[1].is_negated);
        assert_eq!(parsed.terms[2].text, "database");
        assert!(parsed.terms[2].is_negated);
        assert_eq!(parsed.operators[0], QueryOperator::And);
        assert_eq!(parsed.operators[1], QueryOperator::Or);
    }

    #[test]
    fn test_empty_query() {
        let query = TSQuery::new("".to_string());
        let parsed = query.parse();
        assert!(parsed.terms.is_empty());
        assert!(parsed.operators.is_empty());
    }

    #[test]
    fn test_single_term() {
        let query = TSQuery::new("hello".to_string());
        let parsed = query.parse();
        assert_eq!(parsed.terms.len(), 1);
        assert_eq!(parsed.terms[0].text, "hello");
        assert!(parsed.operators.is_empty());
    }

    #[test]
    fn test_complex_query() {
        let query = TSQuery::new("web AND development | !database & programming".to_string());
        let parsed = query.parse();
        assert_eq!(parsed.terms.len(), 4);
        assert_eq!(parsed.operators.len(), 3);
        assert_eq!(parsed.terms[0].text, "web");
        assert_eq!(parsed.terms[1].text, "development");
        assert_eq!(parsed.terms[2].text, "database");
        assert!(parsed.terms[2].is_negated);
        assert_eq!(parsed.terms[3].text, "programming");
        assert_eq!(parsed.operators[0], QueryOperator::And);
        assert_eq!(parsed.operators[1], QueryOperator::Or);
        assert_eq!(parsed.operators[2], QueryOperator::And);
    }
}