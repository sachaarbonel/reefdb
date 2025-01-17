use std::collections::HashMap;
use crate::sql::data_value::DataValue;
use crate::error::ReefDBError;

mod builtins;
pub use builtins::register_builtins;

#[derive(Debug, Clone)]
pub struct Function {
    pub name: String,
    pub args: Vec<FunctionArg>,
    pub return_type: FunctionReturnType,
    pub handler: FunctionHandler,
}

#[derive(Debug, Clone)]
pub struct FunctionArg {
    pub name: String,
    pub arg_type: FunctionArgType,
    pub is_optional: bool,
}

impl Default for FunctionArg {
    fn default() -> Self {
        Self {
            name: String::new(),
            arg_type: FunctionArgType::Any,
            is_optional: false,
        }
    }
}

impl FunctionArg {
    pub fn new(name: String, arg_type: FunctionArgType) -> Self {
        Self {
            name,
            arg_type,
            is_optional: false,
        }
    }

    pub fn optional(mut self) -> Self {
        self.is_optional = true;
        self
    }
}

#[derive(Debug, Clone)]
pub enum FunctionArgType {
    String,
    Integer,
    Float,
    Boolean,
    Any,
    TSVector,
    TSQuery,
}

#[derive(Debug, Clone)]
pub enum FunctionReturnType {
    String,
    Integer,
    Float,
    Boolean,
    Any,
    TSVector,
    TSQuery,
}

pub type FunctionHandler = fn(Vec<DataValue>) -> Result<DataValue, ReefDBError>;

#[derive(Debug, Clone, Default)]
pub struct FunctionRegistry {
    functions: HashMap<String, Function>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    pub fn register(&mut self, function: Function) -> Result<(), ReefDBError> {
        if self.functions.contains_key(&function.name) {
            return Err(ReefDBError::Other(format!("Function {} already registered", function.name)));
        }
        self.functions.insert(function.name.clone(), function);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Function> {
        self.functions.get(name)
    }

    pub fn call(&self, name: &str, args: Vec<DataValue>) -> Result<DataValue, ReefDBError> {
        let function = self.get(name).ok_or_else(|| {
            ReefDBError::Other(format!("Function {} not found", name))
        })?;

        // Count required arguments (non-optional)
        let required_args = function.args.iter().filter(|arg| !arg.is_optional).count();
        let max_args = function.args.len();

        // Validate argument count
        if args.len() < required_args || args.len() > max_args {
            return Err(ReefDBError::Other(format!(
                "Function '{}' expects {} to {} arguments, got {}. Required arguments: {}",
                name,
                required_args,
                max_args,
                args.len(),
                function.args.iter()
                    .filter(|arg| !arg.is_optional)
                    .map(|arg| arg.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        // Validate argument types
        for (i, (arg, provided)) in function.args.iter().zip(args.iter()).enumerate() {
            let type_matches = match (provided, &arg.arg_type) {
                (DataValue::Text(_), FunctionArgType::String) => true,
                (DataValue::Integer(_), FunctionArgType::Integer) => true,
                (DataValue::Float(_), FunctionArgType::Float) => true,
                (DataValue::Boolean(_), FunctionArgType::Boolean) => true,
                (DataValue::TSVector(_), FunctionArgType::TSVector) => true,
                (DataValue::TSQuery(_), FunctionArgType::TSQuery) => true,
                (_, FunctionArgType::Any) => true,
                _ => false,
            };

            if !type_matches {
                return Err(ReefDBError::Other(format!(
                    "Function '{}': argument '{}' (position {}) expects type {:?}, got {:?}",
                    name,
                    arg.name,
                    i + 1,
                    arg.arg_type,
                    provided
                )));
            }
        }

        // Call the function handler with validated arguments
        (function.handler)(args)
    }

    pub fn list_functions(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_registry() {
        let mut registry = FunctionRegistry::new();
        
        // Example function that adds two integers
        let add_function = Function {
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
                    Err(ReefDBError::Other("Invalid argument types".to_string()))
                }
            },
        };

        // Register the function
        registry.register(add_function).unwrap();

        // Test calling the function
        let result = registry.call(
            "add",
            vec![DataValue::Integer(5), DataValue::Integer(3)]
        ).unwrap();

        assert_eq!(result, DataValue::Integer(8));
    }

    #[test]
    fn test_function_error_handling() {
        let mut registry = FunctionRegistry::new();
        
        // Register a function with optional arguments
        let concat_with_sep = Function {
            name: "concat_with_sep".to_string(),
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
                FunctionArg {
                    name: "separator".to_string(),
                    arg_type: FunctionArgType::String,
                    is_optional: true,
                },
            ],
            return_type: FunctionReturnType::String,
            handler: |args| {
                match args.as_slice() {
                    [DataValue::Text(s1), DataValue::Text(s2), DataValue::Text(sep)] => {
                        Ok(DataValue::Text(format!("{}{}{}", s1, sep, s2)))
                    }
                    [DataValue::Text(s1), DataValue::Text(s2)] => {
                        Ok(DataValue::Text(format!("{} {}", s1, s2)))
                    }
                    _ => Err(ReefDBError::Other("Invalid argument types".to_string()))
                }
            },
        };

        registry.register(concat_with_sep).unwrap();

        // Test: Too few arguments
        let err = registry.call(
            "concat_with_sep",
            vec![DataValue::Text("Hello".to_string())]
        ).unwrap_err();
        assert!(err.to_string().contains("expects 2 to 3 arguments, got 1"));
        assert!(err.to_string().contains("Required arguments: str1, str2"));

        // Test: Wrong argument type
        let err = registry.call(
            "concat_with_sep",
            vec![
                DataValue::Text("Hello".to_string()),
                DataValue::Integer(42),
            ]
        ).unwrap_err();
        assert!(err.to_string().contains("argument 'str2' (position 2) expects type String"));

        // Test: Optional argument works
        let result = registry.call(
            "concat_with_sep",
            vec![
                DataValue::Text("Hello".to_string()),
                DataValue::Text("World".to_string()),
            ]
        ).unwrap();
        assert_eq!(result, DataValue::Text("Hello World".to_string()));

        // Test: Optional argument provided
        let result = registry.call(
            "concat_with_sep",
            vec![
                DataValue::Text("Hello".to_string()),
                DataValue::Text("World".to_string()),
                DataValue::Text(", ".to_string()),
            ]
        ).unwrap();
        assert_eq!(result, DataValue::Text("Hello, World".to_string()));
    }
} 