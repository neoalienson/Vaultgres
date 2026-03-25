use crate::catalog::{Catalog, Function, FunctionLanguage, Value};
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::planner::planner::Planner;
use std::sync::Arc;

pub struct SqlFunctionExecutor {
    function: Function,
    params: Vec<Value>,
    catalog: Arc<Catalog>,
    state: SqlFunctionState,
}

enum SqlFunctionState {
    Unexecuted,
    ScalarResult(Option<Value>),
    TableResult { tuples: Vec<Tuple>, current_idx: usize },
}

impl SqlFunctionExecutor {
    pub fn new(function: Function, params: Vec<Value>, catalog: Arc<Catalog>) -> Self {
        Self { function, params, catalog, state: SqlFunctionState::Unexecuted }
    }

    pub fn execute_scalar(&mut self) -> Result<Value, ExecutorError> {
        if self.function.language != FunctionLanguage::Sql {
            return Err(ExecutorError::InternalError(format!(
                "Cannot execute {} language function as scalar",
                match self.function.language {
                    FunctionLanguage::Sql => "SQL",
                    FunctionLanguage::PlPgSql => "PL/pgSQL",
                }
            )));
        }

        let body = self.function.body.trim();

        if body.to_uppercase().starts_with("SELECT") {
            self.execute_select_scalar()
        } else {
            Err(ExecutorError::InternalError(format!("Unsupported SQL function body: {}", body)))
        }
    }

    fn execute_select_scalar(&mut self) -> Result<Value, ExecutorError> {
        let substituted_body = self.substitute_params(&self.function.body)?;

        let mut parser = crate::parser::Parser::new(&substituted_body).map_err(|e| {
            ExecutorError::InternalError(format!("Failed to parse function body: {}", e))
        })?;

        let stmt = parser.parse().map_err(|e| {
            ExecutorError::InternalError(format!("Failed to parse function body: {}", e))
        })?;

        match stmt {
            crate::parser::ast::Statement::Select(select) => {
                let planner = Planner::new_with_catalog(self.catalog.clone());
                let mut plan = planner.plan(&select).map_err(|e| {
                    ExecutorError::InternalError(format!("Failed to plan function body: {}", e))
                })?;

                let mut tuples = Vec::new();
                while let Some(tuple) = plan.next()? {
                    tuples.push(tuple);
                }

                if tuples.is_empty() {
                    self.state = SqlFunctionState::ScalarResult(None);
                    return Ok(Value::Null);
                }

                let first_tuple = &tuples[0];
                if first_tuple.is_empty() {
                    self.state = SqlFunctionState::ScalarResult(None);
                    return Ok(Value::Null);
                }

                let result = first_tuple.values().next().cloned().unwrap_or(Value::Null);
                self.state = SqlFunctionState::ScalarResult(Some(result.clone()));
                Ok(result)
            }
            _ => Err(ExecutorError::InternalError(format!(
                "Function body must be a SELECT statement"
            ))),
        }
    }

    pub fn execute_setof(&mut self) -> Result<Vec<Tuple>, ExecutorError> {
        if self.function.language != FunctionLanguage::Sql {
            return Err(ExecutorError::InternalError(format!(
                "Cannot execute {} language function as setof",
                match self.function.language {
                    FunctionLanguage::Sql => "SQL",
                    FunctionLanguage::PlPgSql => "PL/pgSQL",
                }
            )));
        }

        let substituted_body = self.substitute_params(&self.function.body)?;

        let mut parser = crate::parser::Parser::new(&substituted_body).map_err(|e| {
            ExecutorError::InternalError(format!("Failed to parse function body: {}", e))
        })?;

        let stmt = parser.parse().map_err(|e| {
            ExecutorError::InternalError(format!("Failed to parse function body: {}", e))
        })?;

        match stmt {
            crate::parser::ast::Statement::Select(select) => {
                let planner = Planner::new_with_catalog(self.catalog.clone());
                let mut plan = planner.plan(&select).map_err(|e| {
                    ExecutorError::InternalError(format!("Failed to plan function body: {}", e))
                })?;

                let mut tuples = Vec::new();
                while let Some(tuple) = plan.next()? {
                    tuples.push(tuple);
                }

                Ok(tuples)
            }
            _ => Err(ExecutorError::InternalError(format!(
                "Function body must be a SELECT statement"
            ))),
        }
    }

    fn substitute_params(&self, body: &str) -> Result<String, ExecutorError> {
        let mut result = body.to_string();

        for (i, param) in self.params.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            let value_str = self.value_to_sql_string(param);
            result = result.replace(&placeholder, &value_str);
        }

        Ok(result)
    }

    fn value_to_sql_string(&self, value: &Value) -> String {
        match value {
            Value::Int(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bool(b) => {
                if *b {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            Value::Null => "NULL".to_string(),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| self.value_to_sql_string(v)).collect();
                format!("ARRAY[{}]", items.join(", "))
            }
            Value::Json(j) => format!("'{}'", j.replace('\'', "''")),
            Value::Date(d) => format!("DATE '{}'", d),
            Value::Time(t) => format!("TIME '{}'", t),
            Value::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
            Value::Decimal(v, _) => v.to_string(),
            Value::Bytea(b) => {
                let hex_str: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                format!("'\\x{}'", hex_str)
            }
            Value::Enum(e) => format!("'{}[{}]'", e.type_name, e.index),
            Value::Composite(c) => format!(
                "ROW({})",
                c.fields
                    .iter()
                    .map(|(_, v)| self.value_to_sql_string(v))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::Range(r) => r.to_string(),
        }
    }

    pub fn is_setof(&self) -> bool {
        let rt = &self.function.return_type;
        rt.starts_with("SETOF") || rt.starts_with("TABLE")
    }
}

impl Executor for SqlFunctionExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        match &mut self.state {
            SqlFunctionState::Unexecuted => {
                if self.is_setof() {
                    let tuples = self.execute_setof()?;
                    self.state = SqlFunctionState::TableResult { tuples, current_idx: 0 };
                    return self.next();
                } else {
                    let value = self.execute_scalar()?;
                    self.state = SqlFunctionState::ScalarResult(Some(value));
                    return self.next();
                }
            }
            SqlFunctionState::ScalarResult(Some(_)) => {
                let mut new_state = SqlFunctionState::ScalarResult(None);
                std::mem::swap(&mut self.state, &mut new_state);
                if let SqlFunctionState::ScalarResult(Some(value)) = new_state {
                    let mut tuple = Tuple::new();
                    tuple.insert("".to_string(), value);
                    return Ok(Some(tuple));
                }
                Ok(None)
            }
            SqlFunctionState::ScalarResult(None) => Ok(None),
            SqlFunctionState::TableResult { tuples, current_idx } => {
                if *current_idx < tuples.len() {
                    let tuple = tuples[*current_idx].clone();
                    *current_idx += 1;
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl std::fmt::Debug for SqlFunctionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlFunctionExecutor")
            .field("function", &self.function.name)
            .field("params", &self.params.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_to_sql_string_int() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "test".to_string(),
            parameters: vec![],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 1".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor = SqlFunctionExecutor::new(func, vec![], catalog);
        assert_eq!(executor.value_to_sql_string(&Value::Int(42)), "42");
    }

    #[test]
    fn test_value_to_sql_string_text() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "test".to_string(),
            parameters: vec![],
            return_type: "TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 1".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor = SqlFunctionExecutor::new(func, vec![], catalog);
        assert_eq!(executor.value_to_sql_string(&Value::Text("hello".to_string())), "'hello'");
    }

    #[test]
    fn test_value_to_sql_string_text_with_quotes() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "test".to_string(),
            parameters: vec![],
            return_type: "TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 1".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor = SqlFunctionExecutor::new(func, vec![], catalog);
        assert_eq!(
            executor.value_to_sql_string(&Value::Text("O'Reilly".to_string())),
            "'O''Reilly'"
        );
    }

    #[test]
    fn test_value_to_sql_string_null() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "test".to_string(),
            parameters: vec![],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 1".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor = SqlFunctionExecutor::new(func, vec![], catalog);
        assert_eq!(executor.value_to_sql_string(&Value::Null), "NULL");
    }

    #[test]
    fn test_value_to_sql_string_bool() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "test".to_string(),
            parameters: vec![],
            return_type: "BOOL".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 1".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor = SqlFunctionExecutor::new(func, vec![], catalog);
        assert_eq!(executor.value_to_sql_string(&Value::Bool(true)), "TRUE");
        assert_eq!(executor.value_to_sql_string(&Value::Bool(false)), "FALSE");
    }

    #[test]
    fn test_substitute_params() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "add".to_string(),
            parameters: vec![],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1 + $2".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor = SqlFunctionExecutor::new(func, vec![Value::Int(5), Value::Int(3)], catalog);

        assert_eq!(executor.substitute_params("SELECT $1 + $2").unwrap(), "SELECT 5 + 3");
    }

    #[test]
    fn test_substitute_params_text() {
        let catalog = Arc::new(Catalog::new());
        let func = Function {
            name: "greet".to_string(),
            parameters: vec![],
            return_type: "TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 'Hello, ' || $1".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor =
            SqlFunctionExecutor::new(func, vec![Value::Text("World".to_string())], catalog);

        assert_eq!(
            executor.substitute_params("SELECT 'Hello, ' || $1").unwrap(),
            "SELECT 'Hello, ' || 'World'"
        );
    }

    #[test]
    fn test_is_setof() {
        let catalog = Arc::new(Catalog::new());

        let func_setof = Function {
            name: "get_names".to_string(),
            parameters: vec![],
            return_type: "SETOF TEXT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT name FROM users".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor_setof = SqlFunctionExecutor::new(func_setof, vec![], catalog.clone());
        assert!(executor_setof.is_setof());

        let func_scalar = Function {
            name: "add".to_string(),
            parameters: vec![],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1 + $2".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor_scalar = SqlFunctionExecutor::new(func_scalar, vec![], catalog);
        assert!(!executor_scalar.is_setof());
    }

    #[test]
    fn test_is_table() {
        let catalog = Arc::new(Catalog::new());

        let func_table = Function {
            name: "get_pair".to_string(),
            parameters: vec![],
            return_type: "TABLE(a INT, b TEXT)".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT 1, 'hello'".to_string(),
            is_variadic: false,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
            rows: 1,
        };

        let executor_table = SqlFunctionExecutor::new(func_table, vec![], catalog);
        assert!(executor_table.is_setof());
    }
}
