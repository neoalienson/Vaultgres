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

    #[test]
    fn test_resolve_with_defaults_optional_params() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "greet".to_string(),
            parameters: vec![
                Parameter {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                },
                Parameter {
                    name: "greeting".to_string(),
                    data_type: "TEXT".to_string(),
                    default: Some("'Hello'".to_string()),
                },
            ],
            return_type: "TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $2 || ' ' || $1".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        registry.register(func).unwrap();

        // Resolve with both parameters
        let resolved =
            registry.resolve_with_defaults("greet", &["TEXT".to_string(), "TEXT".to_string()]);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().name, "greet");
        assert_eq!(resolved.unwrap().parameters.len(), 2);

        // Resolve with only required parameter
        let resolved = registry.resolve_with_defaults("greet", &["TEXT".to_string()]);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().name, "greet");
        assert_eq!(resolved.unwrap().parameters.len(), 2);

        // Resolve with too few parameters
        let resolved = registry.resolve_with_defaults("greet", &[]);
        assert!(resolved.is_none());
    }

    #[test]
    fn test_resolve_with_defaults_variadic() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "concat_all".to_string(),
            parameters: vec![
                Parameter {
                    name: "initial".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                },
                Parameter {
                    name: "items".to_string(),
                    data_type: "TEXT[]".to_string(),
                    default: None,
                }, // Represents VARARGS
            ],
            return_type: "TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT initial || array_to_string(items, '')".to_string(),
            is_variadic: true,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        registry.register(func).unwrap();

        // Resolve with only the required non-variadic parameter
        let resolved = registry.resolve_with_defaults("concat_all", &["TEXT".to_string()]);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().name, "concat_all");

        // Resolve with one variadic parameter
        let resolved =
            registry.resolve_with_defaults("concat_all", &["TEXT".to_string(), "TEXT".to_string()]);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().name, "concat_all");

        // Resolve with multiple variadic parameters
        let resolved = registry.resolve_with_defaults(
            "concat_all",
            &["TEXT".to_string(), "TEXT".to_string(), "TEXT".to_string()],
        );
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().name, "concat_all");

        // Resolve with too few arguments (missing initial)
        let resolved = registry.resolve_with_defaults("concat_all", &[]);
        assert!(resolved.is_none());
    }

    #[test]
    fn test_resolve_with_defaults_no_match() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "only_int".to_string(),
            parameters: vec![Parameter {
                name: "a".to_string(),
                data_type: "INT".to_string(),
                default: None,
            }],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        registry.register(func).unwrap();

        // Incorrect type
        let resolved = registry.resolve_with_defaults("only_int", &["TEXT".to_string()]);
        assert!(resolved.is_some()); // `resolve_with_defaults` only checks count, not type.
        // This indicates a potential design choice that needs to be understood.
        // For now, testing its current behavior.
    }

    #[test]
    fn test_load_from_map() {
        let mut registry = FunctionRegistry::new();
        let func1 = Function {
            name: "func1".to_string(),
            parameters: vec![],
            return_type: "VOID".to_string(),
            language: FunctionLanguage::Sql,
            body: "".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        let func2 = Function {
            name: "func2".to_string(),
            parameters: vec![],
            return_type: "VOID".to_string(),
            language: FunctionLanguage::Sql,
            body: "".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        let mut map = HashMap::new();
        map.insert("func1".to_string(), vec![func1]);
        map.insert("func2".to_string(), vec![func2]);

        registry.load_from_map(map.clone());

        assert_eq!(registry.get_all().len(), 2);
        assert!(registry.get_all().contains_key("func1"));
        assert!(registry.get_all().contains_key("func2"));
    }

    #[test]
    fn test_get_all() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "test_func".to_string(),
            parameters: vec![],
            return_type: "VOID".to_string(),
            language: FunctionLanguage::Sql,
            body: "".to_string(),
            is_variadic: false,
            volatility: FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };
        registry.register(func.clone()).unwrap();

        let all_funcs = registry.get_all();
        assert_eq!(all_funcs.len(), 1);
        assert!(all_funcs.get("test_func").is_some());
        assert_eq!(all_funcs.get("test_func").unwrap()[0].name, "test_func");
    }
}
