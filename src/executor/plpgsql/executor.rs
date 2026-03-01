use super::control_flow::ControlFlow;
use super::evaluator::ExprEvaluator;
use crate::catalog::Value;
use crate::parser::plpgsql_ast::PlPgSqlStmt;
use std::collections::HashMap;

pub struct StmtExecutor<'a> {
    variables: &'a mut HashMap<String, Value>,
    query_executor: &'a Option<Box<dyn Fn(&str) -> Result<Vec<HashMap<String, Value>>, String>>>,
}

impl<'a> StmtExecutor<'a> {
    pub fn new(
        variables: &'a mut HashMap<String, Value>,
        query_executor: &'a Option<
            Box<dyn Fn(&str) -> Result<Vec<HashMap<String, Value>>, String>>,
        >,
    ) -> Self {
        Self { variables, query_executor }
    }

    pub fn execute(&mut self, stmt: &PlPgSqlStmt) -> Result<ControlFlow, String> {
        match stmt {
            PlPgSqlStmt::Declare { name, data_type: _, default } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let value =
                    if let Some(expr) = default { evaluator.eval(expr)? } else { Value::Null };
                self.variables.insert(name.clone(), value);
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::Assign { target, value } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let val = evaluator.eval(value)?;
                self.variables.insert(target.clone(), val);
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::If { condition, then_stmts, else_stmts } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let cond = evaluator.eval(condition)?;
                let stmts = if ExprEvaluator::is_true(&cond) { then_stmts } else { else_stmts };
                for s in stmts {
                    match self.execute(s)? {
                        ControlFlow::None => {}
                        flow => return Ok(flow),
                    }
                }
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::While { condition, body } => {
                loop {
                    let evaluator = ExprEvaluator::new(self.variables);
                    if !ExprEvaluator::is_true(&evaluator.eval(condition)?) {
                        break;
                    }
                    for s in body {
                        match self.execute(s)? {
                            ControlFlow::Exit => return Ok(ControlFlow::None),
                            ControlFlow::Continue => break,
                            ControlFlow::Return(val) => return Ok(ControlFlow::Return(val)),
                            ControlFlow::None => {}
                        }
                    }
                }
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::For { var, start, end, body } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let start_val = evaluator.eval(start)?;
                let end_val = evaluator.eval(end)?;

                if let (Value::Int(s), Value::Int(e)) = (start_val, end_val) {
                    for i in s..=e {
                        self.variables.insert(var.clone(), Value::Int(i));
                        for stmt in body {
                            match self.execute(stmt)? {
                                ControlFlow::Exit => return Ok(ControlFlow::None),
                                ControlFlow::Continue => break,
                                ControlFlow::Return(val) => return Ok(ControlFlow::Return(val)),
                                ControlFlow::None => {}
                            }
                        }
                    }
                    Ok(ControlFlow::None)
                } else {
                    Err("FOR loop requires integer bounds".to_string())
                }
            }
            PlPgSqlStmt::ForEach { var, array, body } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let arr_val = evaluator.eval(array)?;
                if let Value::Array(arr) = arr_val {
                    for elem in arr {
                        self.variables.insert(var.clone(), elem);
                        for stmt in body {
                            match self.execute(stmt)? {
                                ControlFlow::Exit => return Ok(ControlFlow::None),
                                ControlFlow::Continue => break,
                                ControlFlow::Return(val) => return Ok(ControlFlow::Return(val)),
                                ControlFlow::None => {}
                            }
                        }
                    }
                    Ok(ControlFlow::None)
                } else {
                    Err("FOREACH requires array".to_string())
                }
            }
            PlPgSqlStmt::ForQuery { var, query, body } => {
                let executor = self
                    .query_executor
                    .as_ref()
                    .ok_or_else(|| "Query executor not configured".to_string())?;
                let rows = executor(query)?;

                for row in rows {
                    for (key, value) in row {
                        self.variables.insert(key, value);
                    }
                    self.variables.insert(var.clone(), Value::Int(1));

                    for stmt in body {
                        match self.execute(stmt)? {
                            ControlFlow::Exit => return Ok(ControlFlow::None),
                            ControlFlow::Continue => break,
                            ControlFlow::Return(val) => return Ok(ControlFlow::Return(val)),
                            ControlFlow::None => {}
                        }
                    }
                }
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::Loop { body } => loop {
                for s in body {
                    match self.execute(s)? {
                        ControlFlow::Exit => return Ok(ControlFlow::None),
                        ControlFlow::Continue => break,
                        ControlFlow::Return(val) => return Ok(ControlFlow::Return(val)),
                        ControlFlow::None => {}
                    }
                }
            },
            PlPgSqlStmt::Exit => Ok(ControlFlow::Exit),
            PlPgSqlStmt::Continue => Ok(ControlFlow::Continue),
            PlPgSqlStmt::Case { expr, when_clauses, else_stmts } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let val = evaluator.eval(expr)?;
                for (when_expr, stmts) in when_clauses {
                    let when_val = evaluator.eval(when_expr)?;
                    if val == when_val {
                        for s in stmts {
                            match self.execute(s)? {
                                ControlFlow::None => {}
                                flow => return Ok(flow),
                            }
                        }
                        return Ok(ControlFlow::None);
                    }
                }
                for s in else_stmts {
                    match self.execute(s)? {
                        ControlFlow::None => {}
                        flow => return Ok(flow),
                    }
                }
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::Return { value } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let val = if let Some(expr) = value { evaluator.eval(expr)? } else { Value::Null };
                Ok(ControlFlow::Return(val))
            }
            PlPgSqlStmt::Execute { query } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let query = evaluator.eval_string(query)?;
                let executor = self
                    .query_executor
                    .as_ref()
                    .ok_or_else(|| "Query executor not configured".to_string())?;
                executor(&query)?;
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::Perform { query } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let query = evaluator.eval_string(query)?;
                let executor = self
                    .query_executor
                    .as_ref()
                    .ok_or_else(|| "Query executor not configured".to_string())?;
                let _ = executor(&query)?;
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::ExceptionBlock { try_stmts, exception_var, catch_stmts } => {
                for stmt in try_stmts {
                    match self.execute(stmt) {
                        Ok(flow) => match flow {
                            ControlFlow::None => {}
                            other => return Ok(other),
                        },
                        Err(e) => {
                            self.variables.insert(exception_var.clone(), Value::Text(e));
                            for catch_stmt in catch_stmts {
                                match self.execute(catch_stmt)? {
                                    ControlFlow::None => {}
                                    flow => return Ok(flow),
                                }
                            }
                            return Ok(ControlFlow::None);
                        }
                    }
                }
                Ok(ControlFlow::None)
            }
            PlPgSqlStmt::Raise { message } => {
                let evaluator = ExprEvaluator::new(self.variables);
                let msg = evaluator.eval_string(message)?;
                Err(msg)
            }
        }
    }
}
