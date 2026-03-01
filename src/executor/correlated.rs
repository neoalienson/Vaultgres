use crate::catalog::Value;

pub enum SubqueryKind {
    Exists,
    In,
    Scalar,
}

pub struct CorrelatedExecutor;

impl CorrelatedExecutor {
    pub fn execute_exists(
        outer_rows: &[Vec<Value>],
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut result = Vec::new();
        for outer_row in outer_rows {
            let subquery_results = subquery_fn(outer_row)?;
            if !subquery_results.is_empty() {
                result.push(outer_row.clone());
            }
        }
        Ok(result)
    }

    pub fn execute_in(
        outer_rows: &[Vec<Value>],
        outer_col_idx: usize,
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut result = Vec::new();
        for outer_row in outer_rows {
            let subquery_results = subquery_fn(outer_row)?;
            let outer_value = &outer_row[outer_col_idx];

            if subquery_results.iter().any(|row| row.get(0) == Some(outer_value)) {
                result.push(outer_row.clone());
            }
        }
        Ok(result)
    }

    pub fn execute_scalar(
        outer_rows: &[Vec<Value>],
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut result = Vec::new();
        for outer_row in outer_rows {
            let subquery_results = subquery_fn(outer_row)?;
            let mut combined = outer_row.clone();

            if let Some(inner_row) = subquery_results.first() {
                combined.extend(inner_row.clone());
            } else {
                combined.push(Value::Null);
            }
            result.push(combined);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exists_with_results() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)]];

        let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            if let Value::Int(n) = row[0] {
                if n == 1 {
                    Ok(vec![vec![Value::Int(100)]])
                } else {
                    Ok(vec![])
                }
            } else {
                Ok(vec![])
            }
        };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![Value::Int(1)]);
    }

    #[test]
    fn test_exists_no_results() {
        let outer = vec![vec![Value::Int(1)]];

        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![]) };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_in_match() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)]];

        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            Ok(vec![vec![Value::Int(1)], vec![Value::Int(3)]])
        };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![Value::Int(1)]);
    }

    #[test]
    fn test_in_no_match() {
        let outer = vec![vec![Value::Int(5)]];

        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            Ok(vec![vec![Value::Int(1)], vec![Value::Int(2)]])
        };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scalar_with_result() {
        let outer = vec![vec![Value::Int(1)]];

        let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            if let Value::Int(n) = row[0] {
                Ok(vec![vec![Value::Int(n * 10)]])
            } else {
                Ok(vec![])
            }
        };

        let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![Value::Int(1), Value::Int(10)]);
    }

    #[test]
    fn test_scalar_no_result() {
        let outer = vec![vec![Value::Int(1)]];

        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![]) };

        let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![Value::Int(1), Value::Null]);
    }
}
