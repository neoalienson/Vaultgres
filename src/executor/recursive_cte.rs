use crate::catalog::Value;
use std::collections::HashSet;

pub struct RecursiveCTEExecutor;

impl RecursiveCTEExecutor {
    pub fn execute(
        base_results: Vec<Vec<Value>>,
        recursive_fn: &dyn Fn(&[Vec<Value>]) -> Result<Vec<Vec<Value>>, String>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut working_table = base_results.clone();
        let mut result = base_results;
        let mut seen = HashSet::new();

        for tuple in &result {
            seen.insert(Self::tuple_key(tuple));
        }

        while !working_table.is_empty() {
            let mut next_working = Vec::new();
            let recursive_results = recursive_fn(&working_table)?;

            for tuple in recursive_results {
                let key = Self::tuple_key(&tuple);
                if seen.insert(key) {
                    next_working.push(tuple.clone());
                    result.push(tuple);
                }
            }

            working_table = next_working;
        }

        Ok(result)
    }

    fn tuple_key(tuple: &[Value]) -> Vec<u8> {
        let mut key = Vec::new();
        for value in tuple {
            match value {
                Value::Int(n) => key.extend_from_slice(&n.to_le_bytes()),
                Value::Float(f) => key.extend_from_slice(&f.to_le_bytes()),
                Value::Bool(b) => key.push(if *b { 1 } else { 0 }),
                Value::Text(s) => key.extend_from_slice(s.as_bytes()),
                Value::Array(a) => {
                    for v in a {
                        match v {
                            Value::Int(n) => key.extend_from_slice(&n.to_le_bytes()),
                            Value::Text(s) => key.extend_from_slice(s.as_bytes()),
                            _ => {}
                        }
                    }
                }
                Value::Json(j) => key.extend_from_slice(j.as_bytes()),
                Value::Null => key.push(0),
            }
            key.push(255);
        }
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_recursive() {
        let base = vec![vec![Value::Int(1)]];

        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let Value::Int(n) = row[0] {
                    if n < 5 {
                        results.push(vec![Value::Int(n + 1)]);
                    }
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], vec![Value::Int(1)]);
        assert_eq!(result[4], vec![Value::Int(5)]);
    }

    #[test]
    fn test_cycle_detection() {
        let base = vec![vec![Value::Int(1)]];

        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let Value::Int(n) = row[0] {
                    results.push(vec![Value::Int((n % 3) + 1)]);
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_empty_base() {
        let base: Vec<Vec<Value>> = vec![];

        let recursive_fn =
            |_: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(1)]]) };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_no_recursion() {
        let base = vec![vec![Value::Int(1)], vec![Value::Int(2)]];

        let recursive_fn = |_: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![]) };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 2);
    }
}
