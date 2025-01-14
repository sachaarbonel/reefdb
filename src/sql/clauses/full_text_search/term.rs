#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTerm {
    pub text: String,
    pub is_negated: bool,
}

impl ParsedTerm {
    pub fn new(text: String, is_negated: bool) -> Self {
        Self { text, is_negated }
    }

    pub fn parse(term: &str, is_negated: bool) -> Option<Self> {
        let term = term.trim();
        if term.is_empty() {
            return None;
        }

        // Check if it's an operator keyword
        match term.to_uppercase().as_str() {
            "AND" | "OR" | "NOT" => None,
            _ => Some(ParsedTerm::new(term.to_string(), is_negated)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_parse() {
        assert_eq!(
            ParsedTerm::parse("hello", false),
            Some(ParsedTerm::new("hello".to_string(), false))
        );
        assert_eq!(
            ParsedTerm::parse("world", true),
            Some(ParsedTerm::new("world".to_string(), true))
        );
        assert_eq!(ParsedTerm::parse("AND", false), None);
        assert_eq!(ParsedTerm::parse("", false), None);
        assert_eq!(ParsedTerm::parse("  ", false), None);
    }
} 