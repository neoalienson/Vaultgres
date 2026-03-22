use crate::catalog::Value;
use crate::parser::ast::BinaryOperator;

pub struct ArraySubqueryExecutor;

impl ArraySubqueryExecutor {
    pub fn execute_any(value: &Value, subquery_results: &[Value]) -> Result<bool, String> {
        for result in subquery_results {
            if value == result {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn execute_all(
        value: &Value,
        op: &BinaryOperator,
        subquery_results: &[Value],
    ) -> Result<bool, String> {
        if subquery_results.is_empty() {
            return Ok(true);
        }

        for result in subquery_results {
            let matches = Self::compare(value, op, result)?;
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn execute_some(value: &Value, subquery_results: &[Value]) -> Result<bool, String> {
        Self::execute_any(value, subquery_results)
    }

    fn compare(left: &Value, op: &BinaryOperator, right: &Value) -> Result<bool, String> {
        match (left, right) {
            (Value::Int(l), Value::Int(r)) => match op {
                BinaryOperator::Equals => Ok(l == r),
                BinaryOperator::NotEquals => Ok(l != r),
                BinaryOperator::LessThan => Ok(l < r),
                BinaryOperator::LessThanOrEqual => Ok(l <= r),
                BinaryOperator::GreaterThan => Ok(l > r),
                BinaryOperator::GreaterThanOrEqual => Ok(l >= r),
                _ => Err(format!("Unsupported operator: {:?}", op)),
            },
            (Value::Text(l), Value::Text(r)) => match op {
                BinaryOperator::Equals => Ok(l == r),
                BinaryOperator::NotEquals => Ok(l != r),
                _ => Err(format!("Unsupported operator for TEXT: {:?}", op)),
            },
            _ => Err("Type mismatch".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_any_found() {
        let value = Value::Int(5);
        let results = vec![Value::Int(1), Value::Int(5), Value::Int(10)];
        assert!(ArraySubqueryExecutor::execute_any(&value, &results).unwrap());
    }

    #[test]
    fn test_any_not_found() {
        let value = Value::Int(7);
        let results = vec![Value::Int(1), Value::Int(5), Value::Int(10)];
        assert!(!ArraySubqueryExecutor::execute_any(&value, &results).unwrap());
    }

    #[test]
    fn test_all_true() {
        let value = Value::Int(10);
        let results = vec![Value::Int(1), Value::Int(5), Value::Int(9)];
        assert!(
            ArraySubqueryExecutor::execute_all(&value, &BinaryOperator::GreaterThan, &results)
                .unwrap()
        );
    }

    #[test]
    fn test_all_false() {
        let value = Value::Int(5);
        let results = vec![Value::Int(1), Value::Int(5), Value::Int(10)];
        assert!(
            !ArraySubqueryExecutor::execute_all(&value, &BinaryOperator::GreaterThan, &results)
                .unwrap()
        );
    }

    #[test]
    fn test_some_found() {
        let value = Value::Int(5);
        let results = vec![Value::Int(1), Value::Int(5), Value::Int(10)];
        assert!(ArraySubqueryExecutor::execute_some(&value, &results).unwrap());
    }

    #[test]
    fn test_all_empty() {
        let value = Value::Int(5);
        let results = vec![];
        assert!(
            ArraySubqueryExecutor::execute_all(&value, &BinaryOperator::GreaterThan, &results)
                .unwrap()
        );
    }
}
