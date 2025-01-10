use std::fmt;

/// Namespace prefixes for different key types
pub const KEY_NAMESPACE_TABLE: &str = "t";     // Table metadata
pub const KEY_NAMESPACE_ROW: &str = "r";       // Row data
pub const KEY_NAMESPACE_INDEX: &str = "i";     // Index data
pub const KEY_NAMESPACE_META: &str = "m";      // System metadata

/// Separator constants
pub const KEY_SEPARATOR: &str = ":";
pub const COMPOSITE_KEY_SEPARATOR: &str = "#";

/// Key format for different types of keys
#[derive(Debug, Clone, PartialEq)]
pub enum KeyFormat {
    /// Table metadata key: t:{table_name}
    Table(String),
    
    /// Row data key: r:{table_name}:{version}:{primary_key}
    Row {
        table_name: String,
        version: u64,
        primary_key: String,
    },
    
    /// Index key: i:{table_name}:{column_name}:{value}
    Index {
        table_name: String,
        column_name: String,
        value: String,
    },
    
    /// System metadata key: m:{key_name}
    Meta(String),
}

impl KeyFormat {
    /// Create a table metadata key
    pub fn table(table_name: &str) -> String {
        format!("{}{}{}", KEY_NAMESPACE_TABLE, KEY_SEPARATOR, table_name)
    }
    
    /// Create a row data key
    pub fn row(table_name: &str, version: u64, primary_key: &str) -> String {
        format!(
            "{}{}{}{}{}{}{}",
            KEY_NAMESPACE_ROW,
            KEY_SEPARATOR,
            table_name,
            KEY_SEPARATOR,
            version,
            KEY_SEPARATOR,
            primary_key
        )
    }
    
    /// Create an index key
    pub fn index(table_name: &str, column_name: &str, value: &str) -> String {
        format!(
            "{}{}{}{}{}{}{}",
            KEY_NAMESPACE_INDEX,
            KEY_SEPARATOR,
            table_name,
            KEY_SEPARATOR,
            column_name,
            KEY_SEPARATOR,
            value
        )
    }
    
    /// Create a metadata key
    pub fn meta(key_name: &str) -> String {
        format!("{}{}{}", KEY_NAMESPACE_META, KEY_SEPARATOR, key_name)
    }
    
    /// Parse a key string into a KeyFormat enum
    pub fn parse(key: &str) -> Option<KeyFormat> {
        let parts: Vec<&str> = key.split(KEY_SEPARATOR).collect();
        match parts.get(0)? {
            &KEY_NAMESPACE_TABLE => Some(KeyFormat::Table(parts.get(1)?.to_string())),
            &KEY_NAMESPACE_ROW => {
                if parts.len() != 4 {
                    return None;
                }
                Some(KeyFormat::Row {
                    table_name: parts[1].to_string(),
                    version: parts[2].parse().ok()?,
                    primary_key: parts[3].to_string(),
                })
            }
            &KEY_NAMESPACE_INDEX => {
                if parts.len() != 4 {
                    return None;
                }
                Some(KeyFormat::Index {
                    table_name: parts[1].to_string(),
                    column_name: parts[2].to_string(),
                    value: parts[3].to_string(),
                })
            }
            &KEY_NAMESPACE_META => Some(KeyFormat::Meta(parts.get(1)?.to_string())),
            _ => None,
        }
    }
}

impl fmt::Display for KeyFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyFormat::Table(name) => write!(f, "{}", KeyFormat::table(name)),
            KeyFormat::Row { table_name, version, primary_key } => {
                write!(f, "{}", KeyFormat::row(table_name, *version, primary_key))
            }
            KeyFormat::Index { table_name, column_name, value } => {
                write!(f, "{}", KeyFormat::index(table_name, column_name, value))
            }
            KeyFormat::Meta(key) => write!(f, "{}", KeyFormat::meta(key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_format() {
        // Test table key
        let table_key = KeyFormat::table("users");
        assert_eq!(table_key, "t:users");
        
        // Test row key
        let row_key = KeyFormat::row("users", 1, "123");
        assert_eq!(row_key, "r:users:1:123");
        
        // Test index key
        let index_key = KeyFormat::index("users", "email", "test@example.com");
        assert_eq!(index_key, "i:users:email:test@example.com");
        
        // Test meta key
        let meta_key = KeyFormat::meta("schema_version");
        assert_eq!(meta_key, "m:schema_version");
    }

    #[test]
    fn test_key_parsing() {
        // Test parsing table key
        let table_key = "t:users";
        let parsed = KeyFormat::parse(table_key).unwrap();
        assert_eq!(parsed, KeyFormat::Table("users".to_string()));
        
        // Test parsing row key
        let row_key = "r:users:1:123";
        let parsed = KeyFormat::parse(row_key).unwrap();
        assert_eq!(parsed, KeyFormat::Row {
            table_name: "users".to_string(),
            version: 1,
            primary_key: "123".to_string(),
        });
        
        // Test parsing index key
        let index_key = "i:users:email:test@example.com";
        let parsed = KeyFormat::parse(index_key).unwrap();
        assert_eq!(parsed, KeyFormat::Index {
            table_name: "users".to_string(),
            column_name: "email".to_string(),
            value: "test@example.com".to_string(),
        });
        
        // Test parsing meta key
        let meta_key = "m:schema_version";
        let parsed = KeyFormat::parse(meta_key).unwrap();
        assert_eq!(parsed, KeyFormat::Meta("schema_version".to_string()));
    }
} 