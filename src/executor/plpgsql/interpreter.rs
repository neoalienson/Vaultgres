use super::control_flow::ControlFlow;
use super::executor::StmtExecutor;
use crate::catalog::Value;
use crate::parser::plpgsql_ast::PlPgSqlFunction;
use std::collections::HashMap;

pub struct PlPgSqlInterpreter {
    variables: HashMap<String, Value>,
    query_executor: Option<Box<dyn Fn(&str) -> Result<Vec<HashMap<String, Value>>, String>>>,
}

impl PlPgSqlInterpreter {
    pub fn new() -> Self {
        Self { variables: HashMap::new(), query_executor: None }
    }

    pub fn with_query_executor<F>(mut self, executor: F) -> Self
    where
        F: Fn(&str) -> Result<Vec<HashMap<String, Value>>, String> + 'static,
    {
        self.query_executor = Some(Box::new(executor));
        self
    }

    pub fn execute(&mut self, func: &PlPgSqlFunction, args: Vec<Value>) -> Result<Value, String> {
        self.variables.clear();

        for (i, value) in args.into_iter().enumerate() {
            self.variables.insert(format!("${}", i + 1), value);
        }

        let mut executor = StmtExecutor::new(&mut self.variables, &self.query_executor);

        for decl in &func.declarations {
            executor.execute(decl)?;
        }

        for stmt in &func.body {
            match executor.execute(stmt)? {
                ControlFlow::Return(val) => return Ok(val),
                ControlFlow::Exit | ControlFlow::Continue => {
                    return Err("EXIT/CONTINUE outside loop".to_string())
                }
                ControlFlow::None => {}
            }
        }

        Ok(Value::Null)
    }
}

impl Default for PlPgSqlInterpreter {
    fn default() -> Self {
        Self::new()
    }
}
