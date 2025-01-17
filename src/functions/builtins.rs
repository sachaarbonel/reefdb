use crate::sql::data_value::DataValue;
use crate::error::ReefDBError;
use crate::functions::{Function, FunctionArg, FunctionArgType, FunctionReturnType, FunctionRegistry};
use std::fmt;
use crate::sql::clauses::full_text_search::ranking::{TSRanking, NORM_LENGTH};
use crate::fts::text_processor::{TextProcessor, TsVector, ProcessedQuery, TSQuery};
use crate::fts::text_processor_impl::DefaultTextProcessor;
use crate::fts::ranking::{RankingSystem, BM25Ranking, RankingConfig};

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Text(s) => write!(f, "{}", s),
            DataValue::Integer(i) => write!(f, "{}", i),
            DataValue::Boolean(b) => write!(f, "{}", b),
            DataValue::Float(fl) => write!(f, "{}", fl),
            DataValue::Date(d) => write!(f, "{}", d),
            DataValue::Timestamp(t) => write!(f, "{}", t),
            DataValue::TSVector(v) => write!(f, "{}", v),
            DataValue::TSQuery(q) => write!(f, "{}", q),
            DataValue::Null => write!(f, "NULL"),
            DataValue::Function { name, args } => write!(f, "Function({:?}, {:?})", name, args),
        }
    }
}

pub fn register_builtins(registry: &mut FunctionRegistry) -> Result<(), ReefDBError> {
    // String functions
    registry.register(Function {
        name: "concat".to_string(),
        args: vec![
            FunctionArg {
                name: "str1".to_string(),
                arg_type: FunctionArgType::String,
                is_optional: false,
            },
            FunctionArg {
                name: "str2".to_string(),
                arg_type: FunctionArgType::String,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::String,
        handler: |args| {
            if let [DataValue::Text(s1), DataValue::Text(s2)] = args.as_slice() {
                Ok(DataValue::Text(format!("{}{}", s1, s2)))
            } else {
                Err(ReefDBError::Other("Invalid argument types for concat".to_string()))
            }
        },
    })?;

    // Numeric functions
    registry.register(Function {
        name: "add".to_string(),
        args: vec![
            FunctionArg {
                name: "a".to_string(),
                arg_type: FunctionArgType::Integer,
                is_optional: false,
            },
            FunctionArg {
                name: "b".to_string(),
                arg_type: FunctionArgType::Integer,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::Integer,
        handler: |args| {
            if let [DataValue::Integer(a), DataValue::Integer(b)] = args.as_slice() {
                Ok(DataValue::Integer(a + b))
            } else {
                Err(ReefDBError::Other("Invalid argument types for add".to_string()))
            }
        },
    })?;

    registry.register(Function {
        name: "multiply".to_string(),
        args: vec![
            FunctionArg {
                name: "a".to_string(),
                arg_type: FunctionArgType::Integer,
                is_optional: false,
            },
            FunctionArg {
                name: "b".to_string(),
                arg_type: FunctionArgType::Integer,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::Integer,
        handler: |args| {
            if let [DataValue::Integer(a), DataValue::Integer(b)] = args.as_slice() {
                Ok(DataValue::Integer(a * b))
            } else {
                Err(ReefDBError::Other("Invalid argument types for multiply".to_string()))
            }
        },
    })?;

    // Full-text search functions
    registry.register(Function {
        name: "to_tsvector".to_string(),
        args: vec![
            FunctionArg {
                name: "text".to_string(),
                arg_type: FunctionArgType::String,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::TSVector,
        handler: |args| {
            if let [DataValue::Text(text)] = args.as_slice() {
                let processor = DefaultTextProcessor::new();
                let vector = processor.process_document(text, None);
                Ok(DataValue::TSVector(vector))
            } else {
                Err(ReefDBError::Other("Invalid argument types for to_tsvector".to_string()))
            }
        },
    })?;

    registry.register(Function {
        name: "to_tsquery".to_string(),
        args: vec![
            FunctionArg {
                name: "query".to_string(),
                arg_type: FunctionArgType::String,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::TSQuery,
        handler: |args| {
            if let [DataValue::Text(query)] = args.as_slice() {
                let processor = DefaultTextProcessor::new();
                let processed = processor.process_query(query, None);
                Ok(DataValue::TSQuery(TSQuery::new(processed.tokens, processed.operators)))
            } else {
                Err(ReefDBError::Other("Invalid argument types for to_tsquery".to_string()))
            }
        },
    })?;

    // Full-text search ranking function
    registry.register(Function {
        name: "ts_rank".to_string(),
        args: vec![
            FunctionArg {
                name: "tsvector".to_string(),
                arg_type: FunctionArgType::TSVector,
                is_optional: false,
            },
            FunctionArg {
                name: "tsquery".to_string(),
                arg_type: FunctionArgType::TSQuery,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::Float,
        handler: |args| {
            if let [DataValue::TSVector(ref vector), DataValue::TSQuery(query)] = args.as_slice() {
                // Create a ranking system with default configuration
                let ranking_system = BM25Ranking::new();
                let config = RankingConfig::default();
                
                // Convert TSQuery to ProcessedQuery
                let processed_query: ProcessedQuery = query.clone().into();
                
                // Calculate the rank using the ProcessedQuery
                let rank = ranking_system.rank(vector, &processed_query, &config);
                
                Ok(DataValue::Float(rank))
            } else {
                Err(ReefDBError::Other("Invalid argument types for ts_rank".to_string()))
            }
        },
    })?;

    // Type conversion functions
    registry.register(Function {
        name: "to_string".to_string(),
        args: vec![
            FunctionArg {
                name: "value".to_string(),
                arg_type: FunctionArgType::Any,
                is_optional: false,
            },
        ],
        return_type: FunctionReturnType::String,
        handler: |args| {
            if let [value] = args.as_slice() {
                Ok(DataValue::Text(value.to_string()))
            } else {
                Err(ReefDBError::Other("Invalid argument count for to_string".to_string()))
            }
        },
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_functions() {
        let mut registry = FunctionRegistry::new();
        register_builtins(&mut registry).unwrap();

        // Test concat
        let result = registry.call(
            "concat",
            vec![
                DataValue::Text("Hello, ".to_string()),
                DataValue::Text("World!".to_string()),
            ],
        ).unwrap();
        assert_eq!(result, DataValue::Text("Hello, World!".to_string()));

        // Test add
        let result = registry.call(
            "add",
            vec![DataValue::Integer(5), DataValue::Integer(3)],
        ).unwrap();
        assert_eq!(result, DataValue::Integer(8));

        // Test multiply
        let result = registry.call(
            "multiply",
            vec![DataValue::Integer(4), DataValue::Integer(3)],
        ).unwrap();
        assert_eq!(result, DataValue::Integer(12));

        // Test to_string
        let result = registry.call(
            "to_string",
            vec![DataValue::Integer(42)],
        ).unwrap();
        assert_eq!(result, DataValue::Text("42".to_string()));
    }
} 