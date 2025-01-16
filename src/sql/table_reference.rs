use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableReference {
    pub name: String,
    pub alias: Option<String>,
}

impl fmt::Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(alias) => write!(f, "{} AS {}", self.name, alias),
            None => write!(f, "{}", self.name),
        }
    }
} 