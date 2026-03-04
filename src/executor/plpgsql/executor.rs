use super::control_flow::ControlFlow;
use super::evaluator::ExprEvaluator;
use crate::catalog::Value;
use crate::parser::ast::Expr;
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
                self.exec_declare(name, default)
            }
            PlPgSqlStmt::Assign { target, value } => self.exec_assign(target, value),
            PlPgSqlStmt::If { condition, then_stmts, else_stmts } => {
                self.exec_if(condition, then_stmts, else_stmts)
            }
            PlPgSqlStmt::While { condition, body } => self.exec_while(condition, body),
            PlPgSqlStmt::For { var, start, end, body } => self.exec_for(var, start, end, body),
            PlPgSqlStmt::ForEach { var, array, body } => self.exec_foreach(var, array, body),
            PlPgSqlStmt::ForQuery { var, query, body } => self.exec_for_query(var, query, body),
            PlPgSqlStmt::Loop { body } => self.exec_loop(body),
            PlPgSqlStmt::Exit => Ok(ControlFlow::Exit),
            PlPgSqlStmt::Continue => Ok(ControlFlow::Continue),
            PlPgSqlStmt::Case { expr, when_clauses, else_stmts } => {
                self.exec_case(expr, when_clauses, else_stmts)
            }
            PlPgSqlStmt::Return { value } => self.exec_return(value),
            PlPgSqlStmt::Execute { query } => self.exec_execute(query),
            PlPgSqlStmt::Perform { query } => self.exec_perform(query),
            PlPgSqlStmt::ExceptionBlock { try_stmts, exception_var, catch_stmts } => {
                self.exec_exception(try_stmts, exception_var, catch_stmts)
            }
            PlPgSqlStmt::Raise { message } => self.exec_raise(message),
        }
    }

    fn exec_declare(&mut self, name: &str, default: &Option<Expr>) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let value = if let Some(expr) = default { evaluator.eval(expr)? } else { Value::Null };
        self.variables.insert(name.to_string(), value);
        Ok(ControlFlow::None)
    }

    fn exec_assign(&mut self, target: &str, value: &Expr) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let val = evaluator.eval(value)?;
        self.variables.insert(target.to_string(), val);
        Ok(ControlFlow::None)
    }

    fn exec_if(
        &mut self,
        condition: &Expr,
        then_stmts: &[PlPgSqlStmt],
        else_stmts: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let cond = evaluator.eval(condition)?;
        let stmts = if ExprEvaluator::is_true(&cond) { then_stmts } else { else_stmts };
        self.exec_stmts(stmts)
    }

    fn exec_while(
        &mut self,
        condition: &Expr,
        body: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        loop {
            let evaluator = ExprEvaluator::new(self.variables);
            if !ExprEvaluator::is_true(&evaluator.eval(condition)?) {
                break;
            }
            match self.exec_loop_body(body)? {
                ControlFlow::None => {}
                flow => return Ok(flow),
            }
        }
        Ok(ControlFlow::None)
    }

    fn exec_for(
        &mut self,
        var: &str,
        start: &Expr,
        end: &Expr,
        body: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let (Value::Int(s), Value::Int(e)) = (evaluator.eval(start)?, evaluator.eval(end)?) else {
            return Err("FOR loop requires integer bounds".to_string());
        };

        for i in s..=e {
            self.variables.insert(var.to_string(), Value::Int(i));
            match self.exec_loop_body(body)? {
                ControlFlow::None => {}
                flow => return Ok(flow),
            }
        }
        Ok(ControlFlow::None)
    }

    fn exec_foreach(
        &mut self,
        var: &str,
        array: &Expr,
        body: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let Value::Array(arr) = evaluator.eval(array)? else {
            return Err("FOREACH requires array".to_string());
        };

        for elem in arr {
            self.variables.insert(var.to_string(), elem);
            match self.exec_loop_body(body)? {
                ControlFlow::None => {}
                flow => return Ok(flow),
            }
        }
        Ok(ControlFlow::None)
    }

    fn exec_for_query(
        &mut self,
        var: &str,
        query: &str,
        body: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        let executor = self
            .query_executor
            .as_ref()
            .ok_or_else(|| "Query executor not configured".to_string())?;
        let rows = executor(query)?;

        for row in rows {
            for (key, value) in row {
                self.variables.insert(key, value);
            }
            self.variables.insert(var.to_string(), Value::Int(1));
            match self.exec_loop_body(body)? {
                ControlFlow::None => {}
                flow => return Ok(flow),
            }
        }
        Ok(ControlFlow::None)
    }

    fn exec_loop(&mut self, body: &[PlPgSqlStmt]) -> Result<ControlFlow, String> {
        loop {
            match self.exec_loop_body(body)? {
                ControlFlow::None => {}
                flow => return Ok(flow),
            }
        }
    }

    fn exec_case(
        &mut self,
        expr: &Expr,
        when_clauses: &[(Expr, Vec<PlPgSqlStmt>)],
        else_stmts: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let val = evaluator.eval(expr)?;

        for (when_expr, stmts) in when_clauses {
            if val == evaluator.eval(when_expr)? {
                return self.exec_stmts(stmts);
            }
        }
        self.exec_stmts(else_stmts)
    }

    fn exec_return(&mut self, value: &Option<Expr>) -> Result<ControlFlow, String> {
        let evaluator = ExprEvaluator::new(self.variables);
        let val = if let Some(expr) = value { evaluator.eval(expr)? } else { Value::Null };
        Ok(ControlFlow::Return(val))
    }

    fn exec_execute(&mut self, query: &str) -> Result<ControlFlow, String> {
        let executor = self
            .query_executor
            .as_ref()
            .ok_or_else(|| "Query executor not configured".to_string())?;
        executor(query)?;
        Ok(ControlFlow::None)
    }

    fn exec_perform(&mut self, query: &str) -> Result<ControlFlow, String> {
        let executor = self
            .query_executor
            .as_ref()
            .ok_or_else(|| "Query executor not configured".to_string())?;
        let _ = executor(query)?;
        Ok(ControlFlow::None)
    }

    fn exec_exception(
        &mut self,
        try_stmts: &[PlPgSqlStmt],
        exception_var: &str,
        catch_stmts: &[PlPgSqlStmt],
    ) -> Result<ControlFlow, String> {
        for stmt in try_stmts {
            match self.execute(stmt) {
                Ok(ControlFlow::None) => {}
                Ok(flow) => return Ok(flow),
                Err(e) => {
                    self.variables.insert(exception_var.to_string(), Value::Text(e));
                    return self.exec_stmts(catch_stmts);
                }
            }
        }
        Ok(ControlFlow::None)
    }

    fn exec_raise(&mut self, message: &str) -> Result<ControlFlow, String> {
        Err(message.to_string())
    }

    fn exec_stmts(&mut self, stmts: &[PlPgSqlStmt]) -> Result<ControlFlow, String> {
        for s in stmts {
            match self.execute(s)? {
                ControlFlow::None => {}
                flow => return Ok(flow),
            }
        }
        Ok(ControlFlow::None)
    }

    fn exec_loop_body(&mut self, body: &[PlPgSqlStmt]) -> Result<ControlFlow, String> {
        for s in body {
            match self.execute(s)? {
                ControlFlow::Exit => return Ok(ControlFlow::None),
                ControlFlow::Continue => return Ok(ControlFlow::None),
                ControlFlow::Return(val) => return Ok(ControlFlow::Return(val)),
                ControlFlow::None => {}
            }
        }
        Ok(ControlFlow::None)
    }
}
