//! ProjectExecutor - Projects specific columns from input tuples

use crate::executor::eval::Eval;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::Expr;

pub struct ProjectExecutor {
    child: Box<dyn Executor>,
    columns: Vec<Expr>,
}

impl ProjectExecutor {
    pub fn new(child: Box<dyn Executor>, columns: Vec<Expr>) -> Self {
        log::debug!("ProjectExecutor::new with {} columns", columns.len());
        for (i, expr) in columns.iter().enumerate() {
            let col_name = Self::get_column_name(expr);
            log::debug!("  [{}] {:?} -> '{}'", i, expr, col_name);

            // Early validation - warn about suspicious column names
            if col_name.contains('{') || col_name.contains("QualifiedColumn") {
                log::warn!(
                    "ProjectExecutor: Suspicious column name '{}' from expression {:?}",
                    col_name,
                    expr
                );
            }
        }

        Self { child, columns }
    }

    /// Get the column name for an expression
    fn get_column_name(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => {
                // Strip table prefix if present (e.g., "o.total" -> "total")
                if let Some(dot_pos) = name.find('.') {
                    name[dot_pos + 1..].to_string()
                } else {
                    name.clone()
                }
            }
            Expr::QualifiedColumn { table: _, column } => {
                // For qualified columns, use just the column name
                column.clone()
            }
            Expr::FunctionCall { name, .. } => name.to_lowercase(),
            Expr::Aggregate { func, .. } => format!("{:?}", func).to_lowercase(),
            Expr::Alias { alias, .. } => alias.clone(),
            Expr::BinaryOp { .. } | Expr::UnaryOp { .. } => {
                // For complex expressions, use a generated name
                format!("{:?}", expr)
            }
            Expr::Number(_) => "number".to_string(),
            Expr::String(_) => "string".to_string(),
            Expr::Star => "*".to_string(),
            _ => format!("expr_{:?}", expr),
        }
    }
}

impl Executor for ProjectExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        match self.child.next()? {
            None => Ok(None),
            Some(input_tuple) => {
                let mut result_tuple = Tuple::new();
                for expr in &self.columns {
                    let evaluated_value = Eval::eval_expr(expr, &input_tuple)?;
                    let col_name = Self::get_column_name(expr);
                    result_tuple.insert(col_name, evaluated_value);
                }
                Ok(Some(result_tuple))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::operators::executor::Tuple;
    use std::collections::HashMap;

    /// Mock executor that returns predefined tuples
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
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    fn test_get_column_name_simple() {
        let expr = Expr::Column("name".to_string());
        assert_eq!(ProjectExecutor::get_column_name(&expr), "name");
    }

    #[test]
    fn test_get_column_name_with_table_prefix() {
        let expr = Expr::Column("o.total".to_string());
        assert_eq!(ProjectExecutor::get_column_name(&expr), "total");
    }

    #[test]
    fn test_get_column_name_with_complex_prefix() {
        let expr = Expr::Column("customer_orders.order_total".to_string());
        assert_eq!(ProjectExecutor::get_column_name(&expr), "order_total");
    }

    #[test]
    fn test_get_column_name_qualified_column() {
        let expr = Expr::QualifiedColumn { table: "o".to_string(), column: "total".to_string() };
        assert_eq!(ProjectExecutor::get_column_name(&expr), "total");
    }

    #[test]
    fn test_get_column_name_alias() {
        let expr = Expr::Alias {
            alias: "order_id".to_string(),
            expr: Box::new(Expr::Column("o.id".to_string())),
        };
        assert_eq!(ProjectExecutor::get_column_name(&expr), "order_id");
    }

    #[test]
    fn test_get_column_name_function() {
        let expr = Expr::FunctionCall { name: "CONCAT".to_string(), args: vec![] };
        assert_eq!(ProjectExecutor::get_column_name(&expr), "concat");
    }

    #[test]
    fn test_project_executor_simple_projection() {
        let mut input_tuple = Tuple::new();
        input_tuple.insert("id".to_string(), crate::catalog::Value::Int(1));
        input_tuple.insert("name".to_string(), crate::catalog::Value::Text("Alice".to_string()));

        let mock_executor = MockExecutor::new(vec![input_tuple]);
        let mut projector = ProjectExecutor::new(
            Box::new(mock_executor),
            vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())],
        );

        let result = projector.next().unwrap().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(result.get("name"), Some(&crate::catalog::Value::Text("Alice".to_string())));
    }

    #[test]
    fn test_project_executor_with_table_prefix() {
        // Input tuple has prefixed column names (as they come from JoinExecutor)
        let mut input_tuple = Tuple::new();
        input_tuple.insert("o.id".to_string(), crate::catalog::Value::Int(1));
        input_tuple.insert("o.total".to_string(), crate::catalog::Value::Int(100));

        let mock_executor = MockExecutor::new(vec![input_tuple]);
        let mut projector = ProjectExecutor::new(
            Box::new(mock_executor),
            vec![Expr::Column("o.id".to_string()), Expr::Column("o.total".to_string())],
        );

        let result = projector.next().unwrap().unwrap();
        // Output should have stripped prefix (o.id -> id, o.total -> total)
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(result.get("total"), Some(&crate::catalog::Value::Int(100)));
    }

    #[test]
    fn test_project_executor_with_alias() {
        // Input tuple has prefixed column names (as they come from JoinExecutor)
        let mut input_tuple = Tuple::new();
        input_tuple.insert("o.id".to_string(), crate::catalog::Value::Int(1));

        let mock_executor = MockExecutor::new(vec![input_tuple]);
        let mut projector = ProjectExecutor::new(
            Box::new(mock_executor),
            vec![Expr::Alias {
                alias: "order_id".to_string(),
                expr: Box::new(Expr::Column("o.id".to_string())),
            }],
        );

        let result = projector.next().unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("order_id"), Some(&crate::catalog::Value::Int(1)));
    }

    #[test]
    fn test_project_executor_empty_input() {
        let mock_executor = MockExecutor::new(vec![]);
        let mut projector =
            ProjectExecutor::new(Box::new(mock_executor), vec![Expr::Column("id".to_string())]);

        let result = projector.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_project_executor_multiple_rows() {
        let mut tuple1 = Tuple::new();
        tuple1.insert("id".to_string(), crate::catalog::Value::Int(1));

        let mut tuple2 = Tuple::new();
        tuple2.insert("id".to_string(), crate::catalog::Value::Int(2));

        let mock_executor = MockExecutor::new(vec![tuple1, tuple2]);
        let mut projector =
            ProjectExecutor::new(Box::new(mock_executor), vec![Expr::Column("id".to_string())]);

        let result1 = projector.next().unwrap().unwrap();
        assert_eq!(result1.get("id"), Some(&crate::catalog::Value::Int(1)));

        let result2 = projector.next().unwrap().unwrap();
        assert_eq!(result2.get("id"), Some(&crate::catalog::Value::Int(2)));

        let result3 = projector.next().unwrap();
        assert!(result3.is_none());
    }
}
