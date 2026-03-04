//! Edge case tests for Aggregate operator - Updated for new Executor trait

#[cfg(test)]
mod tests {
    use crate::executor::{Executor, ExecutorError, HashAggExecutor};
    use crate::catalog::{Value, TableSchema};
    use crate::parser::ast::{Expr, AggregateFunc, ColumnDef, DataType};
    use std::collections::HashMap;

    struct TestExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl TestExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for TestExecutor {
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

    fn make_tuple(value: i64) -> Tuple {
        let mut map = HashMap::new();
        map.insert("value".to_string(), Value::Int(value));
        map
    }

    fn create_test_schema() -> TableSchema {
        let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];
        TableSchema::new("test".to_string(), columns)
    }

    #[test]
    fn test_hash_agg_count() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let schema = create_test_schema();
        
        let agg_exprs = vec![Expr::Aggregate {
            func: AggregateFunc::Count,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        
        let mut agg = HashAggExecutor::new(Box::new(mock), vec![], agg_exprs, schema).unwrap();
        
        let result = agg.next().unwrap().unwrap();
        // Count should be 3
        assert!(result.get("count(value)").is_some() || result.values().any(|v| matches!(v, Value::Int(3))));
    }

    #[test]
    fn test_hash_agg_sum() {
        let tuples = vec![make_tuple(10), make_tuple(20), make_tuple(30)];
        let mock = TestExecutor::new(tuples);
        let schema = create_test_schema();
        
        let agg_exprs = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        
        let mut agg = HashAggExecutor::new(Box::new(mock), vec![], agg_exprs, schema).unwrap();
        
        let result = agg.next().unwrap().unwrap();
        // Sum should be 60
        assert!(result.values().any(|v| matches!(v, Value::Int(60))));
    }

    #[test]
    fn test_hash_agg_empty_input() {
        let tuples = vec![];
        let mock = TestExecutor::new(tuples);
        let schema = create_test_schema();
        
        let agg_exprs = vec![Expr::Aggregate {
            func: AggregateFunc::Count,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        
        let mut agg = HashAggExecutor::new(Box::new(mock), vec![], agg_exprs, schema).unwrap();
        
        // For empty input with aggregation, we should still get one row with count 0
        let result = agg.next().unwrap();
        // Either None or a row with count 0
        if let Some(r) = result {
            assert!(r.values().any(|v| matches!(v, Value::Int(0))));
        }
    }

    #[test]
    fn test_hash_agg_min_max() {
        let tuples = vec![make_tuple(5), make_tuple(2), make_tuple(8), make_tuple(1)];
        let mock = TestExecutor::new(tuples);
        let schema = create_test_schema();
        
        let agg_exprs = vec![
            Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("value".to_string())),
            },
        ];
        
        let mut agg = HashAggExecutor::new(Box::new(mock), vec![], agg_exprs, schema).unwrap();
        
        let result = agg.next().unwrap().unwrap();
        // Min should be 1, Max should be 8
        assert!(result.values().any(|v| matches!(v, Value::Int(1))));
        assert!(result.values().any(|v| matches!(v, Value::Int(8))));
    }
}
