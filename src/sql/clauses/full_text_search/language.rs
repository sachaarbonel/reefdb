#[derive(Debug, Clone, PartialEq)]
pub enum Language {
    English,
    Simple,
    Custom(String),
}

impl Language {
    pub fn as_str(&self) -> &str {
        match self {
            Language::English => "english",
            Language::Simple => "simple",
            Language::Custom(lang) => lang,
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "english" => Language::English,
            "simple" => Language::Simple,
            other => Language::Custom(other.to_string()),
        }
    }
}

impl Default for Language {
    fn default() -> Self {
        Language::English
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_as_str() {
        assert_eq!(Language::English.as_str(), "english");
        assert_eq!(Language::Simple.as_str(), "simple");
        assert_eq!(Language::Custom("spanish".to_string()).as_str(), "spanish");
    }

    #[test]
    fn test_language_from_str() {
        assert_eq!(Language::from_str("english"), Language::English);
        assert_eq!(Language::from_str("ENGLISH"), Language::English);
        assert_eq!(Language::from_str("simple"), Language::Simple);
        assert_eq!(Language::from_str("SIMPLE"), Language::Simple);
        assert_eq!(Language::from_str("spanish"), Language::Custom("spanish".to_string()));
    }

    #[test]
    fn test_language_default() {
        assert_eq!(Language::default(), Language::English);
    }
}