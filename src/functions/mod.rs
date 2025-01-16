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
}

#[derive(Debug, Clone)]
pub enum FunctionArgType {
    String,
    Integer,
    Float,
    Boolean,
    Any,
}

#[derive(Debug, Clone)]
pub enum FunctionReturnType {
    String,
    Integer,
    Float,
    Boolean,
    Any,
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

        if args.len() != function.args.len() {
            return Err(ReefDBError::Other(format!(
                "Function {} expects {} arguments, got {}",
                name,
                function.args.len(),
                args.len()
            )));
        }

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
                },
                FunctionArg {
                    name: "b".to_string(),
                    arg_type: FunctionArgType::Integer,
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
} 