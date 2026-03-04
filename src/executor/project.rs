use super::eval::Eval;
use super::executor::{Executor, ExecutorError, Tuple};
use crate::catalog::Value as CatalogValue;
use crate::parser::ast::Expr;

pub struct Project {
    child: Box<dyn Executor>,
    columns: Vec<Expr>,
}

impl Project {
    pub fn new(child: Box<dyn Executor>, columns: Vec<Expr>) -> Self {
        Self { child, columns }
    }
}

fn from_catalog_value(val: CatalogValue) -> Vec<u8> {
    match val {
        CatalogValue::Text(s) => s.into_bytes(),
        CatalogValue::Int(i) => i.to_string().into_bytes(),
        _ => format!("{:?}", val).into_bytes(),
    }
}

impl Executor for Project {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        match self.child.next()? {
            None => Ok(None),
            Some(tuple) => {
                let mut result = Tuple::new();
                for expr in &self.columns {
                    if let Expr::Star = expr {
                        return Ok(Some(tuple));
                    }
                    let value = Eval::eval_expr(expr, &tuple)?;
                    let col_name = match expr {
                        Expr::Column(name) => name.clone(),
                        Expr::FunctionCall { name, .. } => name.clone().to_lowercase(),
                        _ => "?".to_string(),
                    };
                    result.insert(col_name, from_catalog_value(value));
                }
                Ok(Some(result))
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.child.close()
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr};
    use std::collections::HashMap;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    #[test]
    fn test_project_single_column() {
        let mut tuple = HashMap::new();
        tuple.insert("id".to_string(), b"1".to_vec());
        tuple.insert("name".to_string(), b"test".to_vec());

        let mock = OldMockExecutor::new(vec![tuple]);
        let mut project = Project::new(Box::new(mock), vec![Expr::Column("id".to_string())]);

        project.open().unwrap();
        let result = project.next().unwrap().unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get("id").unwrap(), b"1");
        assert!(result.get("name").is_none());
    }

    #[test]
    fn test_project_star() {
        let mut tuple = HashMap::new();
        tuple.insert("id".to_string(), b"1".to_vec());
        tuple.insert("name".to_string(), b"test".to_vec());

        let mock = OldMockExecutor::new(vec![tuple.clone()]);
        let mut project = Project::new(Box::new(mock), vec![Expr::Star]);

        project.open().unwrap();
        let result = project.next().unwrap().unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result, tuple);
    }
}
