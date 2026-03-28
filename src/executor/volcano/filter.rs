//! FilterExecutor - Filters tuples based on a predicate

use crate::catalog::{Catalog, Value};
use crate::executor::eval::Eval;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
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
                            None,
                            None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::operators::executor::Tuple;
    use crate::parser::ast::Expr;
    use std::collections::VecDeque;

    // Mock executor for testing
    struct MockExecutor {
        data: VecDeque<Tuple>,
    }

    impl MockExecutor {
        fn new(data: Vec<Tuple>) -> Self {
            Self { data: VecDeque::from(data) }
        }
    }

    impl Executor for MockExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            Ok(self.data.pop_front())
        }
    }

    fn create_tuple(values: Vec<(&str, Value)>) -> Tuple {
        let mut tuple = HashMap::new();
        for (key, value) in values {
            tuple.insert(key.to_string(), value);
        }
        tuple
    }

    use std::collections::HashMap;

    #[test]
    fn test_filter_is_null_with_null_value() {
        // Create tuple with NULL value
        let tuple = create_tuple(vec![
            ("id", Value::Int(1)),
            ("value", Value::Null),
            ("txt", Value::Text("test".to_string())),
        ]);

        let mock = MockExecutor::new(vec![tuple]);
        let predicate = Expr::IsNull(Box::new(Expr::Column("value".to_string())));
        let mut filter = FilterExecutor::new(Box::new(mock), predicate);

        // Should return the tuple because value IS NULL
        let result = filter.next().unwrap();
        assert!(result.is_some());
        let returned_tuple = result.unwrap();
        assert_eq!(returned_tuple.get("id"), Some(&Value::Int(1)));
        assert_eq!(returned_tuple.get("value"), Some(&Value::Null));

        // No more tuples
        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_is_null_with_non_null_value() {
        // Create tuple with non-NULL value
        let tuple = create_tuple(vec![
            ("id", Value::Int(1)),
            ("value", Value::Int(100)),
            ("txt", Value::Text("test".to_string())),
        ]);

        let mock = MockExecutor::new(vec![tuple]);
        let predicate = Expr::IsNull(Box::new(Expr::Column("value".to_string())));
        let mut filter = FilterExecutor::new(Box::new(mock), predicate);

        // Should not return the tuple because value is NOT NULL
        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_is_null_mixed_values() {
        // Create tuples with mixed NULL and non-NULL values
        let tuples = vec![
            create_tuple(vec![
                ("id", Value::Int(1)),
                ("value", Value::Int(10)),
                ("txt", Value::Text("hello".to_string())),
            ]),
            create_tuple(vec![
                ("id", Value::Int(2)),
                ("value", Value::Null),
                ("txt", Value::Text("world".to_string())),
            ]),
            create_tuple(vec![
                ("id", Value::Int(3)),
                ("value", Value::Int(30)),
                ("txt", Value::Null),
            ]),
        ];

        let mock = MockExecutor::new(tuples);
        let predicate = Expr::IsNull(Box::new(Expr::Column("value".to_string())));
        let mut filter = FilterExecutor::new(Box::new(mock), predicate);

        // Should only return tuple with id=2 (where value IS NULL)
        let result = filter.next().unwrap().unwrap();
        assert_eq!(result.get("id"), Some(&Value::Int(2)));
        assert_eq!(result.get("value"), Some(&Value::Null));

        // No more matching tuples
        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_is_not_null_with_null_value() {
        let tuple = create_tuple(vec![("id", Value::Int(1)), ("value", Value::Null)]);

        let mock = MockExecutor::new(vec![tuple]);
        let predicate = Expr::IsNotNull(Box::new(Expr::Column("value".to_string())));
        let mut filter = FilterExecutor::new(Box::new(mock), predicate);

        // Should not return the tuple because value IS NULL (not IS NOT NULL)
        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_is_not_null_with_non_null_value() {
        let tuple = create_tuple(vec![("id", Value::Int(1)), ("value", Value::Int(100))]);

        let mock = MockExecutor::new(vec![tuple]);
        let predicate = Expr::IsNotNull(Box::new(Expr::Column("value".to_string())));
        let mut filter = FilterExecutor::new(Box::new(mock), predicate);

        // Should return the tuple because value IS NOT NULL
        let result = filter.next().unwrap().unwrap();
        assert_eq!(result.get("value"), Some(&Value::Int(100)));

        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_is_not_null_mixed_values() {
        let tuples = vec![
            create_tuple(vec![("id", Value::Int(1)), ("value", Value::Int(10))]),
            create_tuple(vec![("id", Value::Int(2)), ("value", Value::Null)]),
            create_tuple(vec![("id", Value::Int(3)), ("value", Value::Int(30))]),
            create_tuple(vec![("id", Value::Int(4)), ("value", Value::Null)]),
        ];

        let mock = MockExecutor::new(tuples);
        let predicate = Expr::IsNotNull(Box::new(Expr::Column("value".to_string())));
        let mut filter = FilterExecutor::new(Box::new(mock), predicate);

        // Should return tuples with id=1 and id=3
        let result1 = filter.next().unwrap().unwrap();
        assert_eq!(result1.get("id"), Some(&Value::Int(1)));

        let result2 = filter.next().unwrap().unwrap();
        assert_eq!(result2.get("id"), Some(&Value::Int(3)));

        assert!(filter.next().unwrap().is_none());
    }

    #[test]
    fn test_filter_multiple_null_columns() {
        let tuples = vec![
            create_tuple(vec![
                ("id", Value::Int(1)),
                ("a", Value::Null),
                ("b", Value::Text("x".to_string())),
            ]),
            create_tuple(vec![
                ("id", Value::Int(2)),
                ("a", Value::Text("y".to_string())),
                ("b", Value::Null),
            ]),
            create_tuple(vec![("id", Value::Int(3)), ("a", Value::Null), ("b", Value::Null)]),
        ];

        // Filter WHERE a IS NULL
        let mock1 = MockExecutor::new(tuples.clone());
        let predicate1 = Expr::IsNull(Box::new(Expr::Column("a".to_string())));
        let mut filter1 = FilterExecutor::new(Box::new(mock1), predicate1);

        let result1 = filter1.next().unwrap().unwrap();
        assert_eq!(result1.get("id"), Some(&Value::Int(1)));

        let result2 = filter1.next().unwrap().unwrap();
        assert_eq!(result2.get("id"), Some(&Value::Int(3)));

        assert!(filter1.next().unwrap().is_none());

        // Filter WHERE b IS NULL
        let mock2 = MockExecutor::new(tuples);
        let predicate2 = Expr::IsNull(Box::new(Expr::Column("b".to_string())));
        let mut filter2 = FilterExecutor::new(Box::new(mock2), predicate2);

        let result3 = filter2.next().unwrap().unwrap();
        assert_eq!(result3.get("id"), Some(&Value::Int(2)));

        let result4 = filter2.next().unwrap().unwrap();
        assert_eq!(result4.get("id"), Some(&Value::Int(3)));

        assert!(filter2.next().unwrap().is_none());
    }
}
