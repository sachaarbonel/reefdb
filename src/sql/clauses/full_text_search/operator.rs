#[derive(Debug, Clone, PartialEq)]
pub enum QueryOperator {
    And,
    Or,
    Not,
}

impl QueryOperator {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "AND" => Some(QueryOperator::And),
            "OR" => Some(QueryOperator::Or),
            "NOT" => Some(QueryOperator::Not),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            QueryOperator::And => "AND",
            QueryOperator::Or => "OR",
            QueryOperator::Not => "NOT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_from_str() {
        assert_eq!(QueryOperator::from_str("AND"), Some(QueryOperator::And));
        assert_eq!(QueryOperator::from_str("OR"), Some(QueryOperator::Or));
        assert_eq!(QueryOperator::from_str("NOT"), Some(QueryOperator::Not));
        assert_eq!(QueryOperator::from_str("INVALID"), None);
    }

    #[test]
    fn test_operator_as_str() {
        assert_eq!(QueryOperator::And.as_str(), "AND");
        assert_eq!(QueryOperator::Or.as_str(), "OR");
        assert_eq!(QueryOperator::Not.as_str(), "NOT");
    }
} 