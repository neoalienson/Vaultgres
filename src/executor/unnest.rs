use crate::catalog::Value;

pub struct UnnestExecutor;

impl UnnestExecutor {
    pub fn execute(array: Value) -> Result<Vec<Vec<Value>>, String> {
        match array {
            Value::Array(arr) => Ok(arr.into_iter().map(|v| vec![v]).collect()),
            _ => Err("unnest requires array".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unnest_simple() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = UnnestExecutor::execute(arr).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], vec![Value::Int(1)]);
        assert_eq!(result[1], vec![Value::Int(2)]);
        assert_eq!(result[2], vec![Value::Int(3)]);
    }

    #[test]
    fn test_unnest_empty() {
        let arr = Value::Array(vec![]);
        let result = UnnestExecutor::execute(arr).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_unnest_mixed_types() {
        let arr =
            Value::Array(vec![Value::Int(1), Value::Text("hello".to_string()), Value::Bool(true)]);
        let result = UnnestExecutor::execute(arr).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], vec![Value::Int(1)]);
        assert_eq!(result[1], vec![Value::Text("hello".to_string())]);
        assert_eq!(result[2], vec![Value::Bool(true)]);
    }
}
