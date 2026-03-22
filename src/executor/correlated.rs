use crate::catalog::Value;

pub enum SubqueryKind {
    Exists,
    NotExists,
    In,
    NotIn,
    Scalar,
}

pub struct CorrelatedExecutor;

impl CorrelatedExecutor {
    fn filter_rows(
        outer_rows: &[Vec<Value>],
        predicate: impl Fn(&[Value], &[Vec<Value>]) -> bool,
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut result = Vec::new();
        for outer_row in outer_rows {
            let subquery_results = subquery_fn(outer_row)?;
            if predicate(outer_row, &subquery_results) {
                result.push(outer_row.clone());
            }
        }
        Ok(result)
    }

    pub fn execute_not_exists(
        outer_rows: &[Vec<Value>],
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        Self::filter_rows(outer_rows, |_, results| results.is_empty(), subquery_fn)
    }

    pub fn execute_exists(
        outer_rows: &[Vec<Value>],
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        Self::filter_rows(outer_rows, |_, results| !results.is_empty(), subquery_fn)
    }

    pub fn execute_not_in(
        outer_rows: &[Vec<Value>],
        outer_col_idx: usize,
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        Self::filter_rows(
            outer_rows,
            |outer_row, results| {
                !results.iter().any(|row| row.first() == Some(&outer_row[outer_col_idx]))
            },
            subquery_fn,
        )
    }

    pub fn execute_in(
        outer_rows: &[Vec<Value>],
        outer_col_idx: usize,
        subquery_fn: &dyn Fn(&[Value]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        Self::filter_rows(
            outer_rows,
            |outer_row, results| {
                results.iter().any(|row| row.first() == Some(&outer_row[outer_col_idx]))
            },
            subquery_fn,
        )
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
    fn test_not_exists_with_results() {
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

        let result = CorrelatedExecutor::execute_not_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![Value::Int(2)]);
    }

    #[test]
    fn test_not_in_match() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(5)]];
        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            Ok(vec![vec![Value::Int(1)], vec![Value::Int(3)]])
        };

        let result = CorrelatedExecutor::execute_not_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![Value::Int(2)]);
        assert_eq!(result[1], vec![Value::Int(5)]);
    }

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

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn test_exists_empty_outer() {
        let outer: Vec<Vec<Value>> = vec![];
        let subquery_fn =
            |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(1)]]) };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_exists_all_match() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];
        let subquery_fn =
            |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(100)]]) };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_exists_none_match() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![]) };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_in_empty_outer() {
        let outer: Vec<Vec<Value>> = vec![];
        let subquery_fn =
            |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(1)]]) };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_in_empty_subquery() {
        let outer = vec![vec![Value::Int(1)]];
        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![]) };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_in_multiple_matches() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];
        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            Ok(vec![vec![Value::Int(1)], vec![Value::Int(2)]])
        };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_in_with_nulls() {
        let outer = vec![vec![Value::Null], vec![Value::Int(1)]];
        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            Ok(vec![vec![Value::Null], vec![Value::Int(1)]])
        };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_scalar_empty_outer() {
        let outer: Vec<Vec<Value>> = vec![];
        let subquery_fn =
            |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(1)]]) };

        let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scalar_multiple_outer_rows() {
        let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];
        let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            if let Value::Int(n) = row[0] {
                Ok(vec![vec![Value::Int(n * 100)]])
            } else {
                Ok(vec![])
            }
        };

        let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], vec![Value::Int(1), Value::Int(100)]);
        assert_eq!(result[1], vec![Value::Int(2), Value::Int(200)]);
        assert_eq!(result[2], vec![Value::Int(3), Value::Int(300)]);
    }

    #[test]
    fn test_scalar_mixed_results() {
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

        let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![Value::Int(1), Value::Int(100)]);
        assert_eq!(result[1], vec![Value::Int(2), Value::Null]);
    }

    #[test]
    fn test_exists_with_text_values() {
        let outer = vec![vec![Value::Text("a".to_string())], vec![Value::Text("b".to_string())]];
        let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            if let Value::Text(s) = &row[0] {
                if s == "a" {
                    Ok(vec![vec![Value::Int(1)]])
                } else {
                    Ok(vec![])
                }
            } else {
                Ok(vec![])
            }
        };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_in_large_subquery_result() {
        let outer = vec![vec![Value::Int(50)]];
        let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            Ok((1..=100).map(|i| vec![Value::Int(i)]).collect())
        };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_scalar_multiple_columns() {
        let outer = vec![vec![Value::Int(1), Value::Text("a".to_string())]];
        let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
            if let Value::Int(n) = row[0] {
                Ok(vec![vec![Value::Int(n * 10), Value::Text("result".to_string())]])
            } else {
                Ok(vec![])
            }
        };

        let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 4);
    }
}
