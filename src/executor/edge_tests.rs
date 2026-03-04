//! Edge case tests for query executor - Updated for new Executor trait

#[cfg(test)]
mod tests {
    use crate::executor::*;
    use crate::catalog::Value;
    use std::collections::HashMap;

    struct EmptyExecutor;

    impl Executor for EmptyExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            Ok(None)
        }
    }

    struct ErrorExecutor;

    impl Executor for ErrorExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            Err(ExecutorError::Storage("Next failed".to_string()))
        }
    }

    #[test]
    fn test_empty_executor() {
        let mut exec = EmptyExecutor;
        let result = exec.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_executor_next_error() {
        let mut exec = ErrorExecutor;
        assert!(exec.next().is_err());
    }

    #[test]
    fn test_tuple_with_empty_columns() {
        let tuple: Tuple = HashMap::new();
        assert_eq!(tuple.len(), 0);
    }

    #[test]
    fn test_tuple_with_empty_values() {
        let mut tuple: Tuple = HashMap::new();
        tuple.insert("col1".to_string(), Value::Text(String::new()));
        assert!(matches!(tuple.get("col1").unwrap(), Value::Text(s) if s.is_empty()));
    }

    #[test]
    fn test_tuple_with_large_values() {
        let mut tuple: Tuple = HashMap::new();
        tuple.insert("col1".to_string(), Value::Text("x".repeat(1_000_000)));
        if let Value::Text(s) = tuple.get("col1").unwrap() {
            assert_eq!(s.len(), 1_000_000);
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_executor_error_display() {
        let err = ExecutorError::ColumnNotFound("test_col".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("test_col"));
    }

    #[test]
    fn test_executor_error_storage() {
        let err = ExecutorError::Storage("disk error".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("disk error"));
    }

    #[test]
    fn test_executor_error_type_mismatch() {
        let err = ExecutorError::TypeMismatch("expected int, got string".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("expected int"));
    }

    #[test]
    fn test_tuple_column_access_nonexistent() {
        let tuple: Tuple = HashMap::new();
        assert!(tuple.get("nonexistent").is_none());
    }
}
