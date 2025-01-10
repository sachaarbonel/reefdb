use super::Language;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Plain,      // plainto_tsquery
    Phrase,     // phraseto_tsquery
    WebStyle,   // websearch_to_tsquery
    Raw,        // to_tsquery
}

#[derive(Debug, Clone, PartialEq)]
pub struct TSQuery {
    pub text: String,
    pub query_type: QueryType,
    pub language: Option<Language>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryOperator {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTerm {
    pub text: String,
    pub is_negated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTSQuery {
    pub terms: Vec<ParsedTerm>,
    pub operators: Vec<QueryOperator>,
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

    pub fn parse(&self) -> ParsedTSQuery {
        let mut terms = Vec::new();
        let mut operators = Vec::new();
        let mut current_term = String::new();
        let mut is_negated = false;
        let mut chars = self.text.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '!' => {
                    is_negated = true;
                }
                ' ' => {
                    if !current_term.is_empty() {
                        let term = current_term.trim().to_string();
                        match term.to_uppercase().as_str() {
                            "AND" => {
                                operators.push(QueryOperator::And);
                            },
                            "OR" => {
                                operators.push(QueryOperator::Or);
                            },
                            _ => {
                                terms.push(ParsedTerm {
                                    text: term,
                                    is_negated,
                                });
                                is_negated = false;
                            }
                        }
                        current_term = String::new();
                    }
                }
                '&' => {
                    if !current_term.is_empty() {
                        terms.push(ParsedTerm {
                            text: current_term.trim().to_string(),
                            is_negated,
                        });
                        current_term = String::new();
                        is_negated = false;
                    }
                    operators.push(QueryOperator::And);
                }
                '|' => {
                    if !current_term.is_empty() {
                        terms.push(ParsedTerm {
                            text: current_term.trim().to_string(),
                            is_negated,
                        });
                        current_term = String::new();
                        is_negated = false;
                    }
                    operators.push(QueryOperator::Or);
                }
                _ => {
                    current_term.push(c);
                }
            }
        }

        if !current_term.is_empty() {
            let term = current_term.trim().to_string();
            match term.to_uppercase().as_str() {
                "AND" => {
                    operators.push(QueryOperator::And);
                },
                "OR" => {
                    operators.push(QueryOperator::Or);
                },
                _ => {
                    terms.push(ParsedTerm {
                        text: term,
                        is_negated,
                    });
                }
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
}