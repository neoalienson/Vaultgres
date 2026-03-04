use super::executor::{Executor, ExecutorError, Tuple};
use crate::catalog::{Catalog, Value};
use crate::executor::eval::Eval;
use crate::parser::ast::Expr;
use std::sync::Arc;

pub struct FilterExecutor {
    child: Box<dyn Executor>,
    predicate: Expr,
    catalog: Option<Arc<Catalog>>,
}

impl FilterExecutor {
    pub fn new(child: Box<dyn Executor>, predicate: Expr) -> Self {
        Self { child, predicate, catalog: None }
    }

    pub fn with_catalog(mut self, catalog: Arc<Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }
}

impl Executor for FilterExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    let result = if let Some(ref catalog) = self.catalog {
                        Eval::eval_expr_with_catalog(
                            &self.predicate,
                            &tuple,
                            Some(catalog.as_ref()),
                        )?
                    } else {
                        Eval::eval_expr(&self.predicate, &tuple)?
                    };

                    if let Value::Bool(matches) = result {
                        if matches {
                            return Ok(Some(tuple));
                        }
                        // If false, continue to next tuple
                    } else {
                        return Err(ExecutorError::TypeMismatch(
                            "Predicate did not evaluate to a boolean".to_string(),
                        ));
                    }
                }
            }
        }
    }
}
