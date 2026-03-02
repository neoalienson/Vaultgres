use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FunctionLanguage {
    Sql,
    PlPgSql,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Parameter {
    pub name: String,
    pub data_type: String,
    pub default: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Function {
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub return_type: String,
    pub language: FunctionLanguage,
    pub body: String,
    pub is_variadic: bool,
    pub volatility: FunctionVolatility,
    pub cost: f64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    #[default]
    Volatile,
}

pub struct FunctionRegistry {
    functions: HashMap<String, Vec<Function>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self { functions: HashMap::new() }
    }

    pub fn load_from_map(&mut self, functions: HashMap<String, Vec<Function>>) {
        self.functions = functions;
    }

    pub fn get_all(&self) -> &HashMap<String, Vec<Function>> {
        &self.functions
    }

    pub fn register(&mut self, func: Function) -> Result<(), String> {
        self.functions.entry(func.name.clone()).or_default().push(func);
        Ok(())
    }

    pub fn resolve(&self, name: &str, arg_types: &[String]) -> Option<&Function> {
        self.functions.get(name)?.iter().find(|f| self.matches_signature(f, arg_types))
    }

    pub fn resolve_with_defaults(&self, name: &str, arg_types: &[String]) -> Option<&Function> {
        self.functions.get(name)?.iter().find(|f| {
            if f.is_variadic {
                arg_types.len() >= f.parameters.len() - 1
            } else {
                let required = f.parameters.iter().filter(|p| p.default.is_none()).count();
                arg_types.len() >= required && arg_types.len() <= f.parameters.len()
            }
        })
    }

    fn matches_signature(&self, func: &Function, arg_types: &[String]) -> bool {
        func.parameters.len() == arg_types.len()
            && func.parameters.iter().zip(arg_types).all(|(p, t)| &p.data_type == t)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_function() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "add".to_string(),
            parameters: vec![
                Parameter { name: "a".to_string(), data_type: "INT".to_string(), default: None },
                Parameter { name: "b".to_string(), data_type: "INT".to_string(), default: None },
            ],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1 + $2".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        assert!(registry.register(func).is_ok());
    }

    #[test]
    fn test_resolve_function() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "add".to_string(),
            parameters: vec![
                Parameter { name: "a".to_string(), data_type: "INT".to_string(), default: None },
                Parameter { name: "b".to_string(), data_type: "INT".to_string(), default: None },
            ],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1 + $2".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        registry.register(func).unwrap();

        let resolved = registry.resolve("add", &["INT".to_string(), "INT".to_string()]);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().name, "add");
    }

    #[test]
    fn test_resolve_nonexistent() {
        let registry = FunctionRegistry::new();
        assert!(registry.resolve("nonexistent", &[]).is_none());
    }

    #[test]
    fn test_overload_resolution() {
        let mut registry = FunctionRegistry::new();

        let func1 = Function {
            name: "add".to_string(),
            parameters: vec![
                Parameter { name: "a".to_string(), data_type: "INT".to_string(), default: None },
                Parameter { name: "b".to_string(), data_type: "INT".to_string(), default: None },
            ],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1 + $2".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let func2 = Function {
            name: "add".to_string(),
            parameters: vec![
                Parameter { name: "a".to_string(), data_type: "TEXT".to_string(), default: None },
                Parameter { name: "b".to_string(), data_type: "TEXT".to_string(), default: None },
            ],
            return_type: "TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1 || $2".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Stable,
            cost: 100.0,
            rows: 1,
        };

        registry.register(func1).unwrap();
        registry.register(func2).unwrap();

        let int_func = registry.resolve("add", &["INT".to_string(), "INT".to_string()]);
        assert!(int_func.is_some());
        assert_eq!(int_func.unwrap().return_type, "INT");

        let text_func = registry.resolve("add", &["TEXT".to_string(), "TEXT".to_string()]);
        assert!(text_func.is_some());
        assert_eq!(text_func.unwrap().return_type, "TEXT");
    }
}
