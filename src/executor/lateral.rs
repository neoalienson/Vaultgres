use crate::catalog::Value;
use std::collections::HashMap;

pub struct LateralSubqueryExecutor;

impl LateralSubqueryExecutor {
    pub fn execute<F>(
        outer_rows: Vec<HashMap<String, Value>>,
        subquery_fn: F,
    ) -> Result<Vec<HashMap<String, Value>>, String>
    where
        F: Fn(&HashMap<String, Value>) -> Result<Vec<HashMap<String, Value>>, String>,
    {
        let mut results = Vec::new();

        for outer_row in outer_rows {
            let subquery_results = subquery_fn(&outer_row)?;

            for mut subquery_row in subquery_results {
                for (key, value) in &outer_row {
                    subquery_row.insert(key.clone(), value.clone());
                }
                results.push(subquery_row);
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lateral_simple() {
        let mut outer1 = HashMap::new();
        outer1.insert("id".to_string(), Value::Int(1));

        let mut outer2 = HashMap::new();
        outer2.insert("id".to_string(), Value::Int(2));

        let outer_rows = vec![outer1, outer2];

        let subquery_fn = |outer: &HashMap<String, Value>| {
            let id = match outer.get("id") {
                Some(Value::Int(n)) => *n,
                _ => return Err("Invalid id".to_string()),
            };

            let mut row = HashMap::new();
            row.insert("value".to_string(), Value::Int(id * 10));
            Ok(vec![row])
        };

        let results = LateralSubqueryExecutor::execute(outer_rows, subquery_fn).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("value"), Some(&Value::Int(10)));
        assert_eq!(results[1].get("value"), Some(&Value::Int(20)));
    }

    #[test]
    fn test_lateral_multiple_results() {
        let mut outer = HashMap::new();
        outer.insert("id".to_string(), Value::Int(1));

        let subquery_fn = |_: &HashMap<String, Value>| {
            let mut row1 = HashMap::new();
            row1.insert("value".to_string(), Value::Int(10));

            let mut row2 = HashMap::new();
            row2.insert("value".to_string(), Value::Int(20));

            Ok(vec![row1, row2])
        };

        let results = LateralSubqueryExecutor::execute(vec![outer], subquery_fn).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_lateral_empty_subquery() {
        let mut outer = HashMap::new();
        outer.insert("id".to_string(), Value::Int(1));

        let subquery_fn = |_: &HashMap<String, Value>| Ok(vec![]);

        let results = LateralSubqueryExecutor::execute(vec![outer], subquery_fn).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_lateral_preserves_outer_columns() {
        let mut outer = HashMap::new();
        outer.insert("id".to_string(), Value::Int(1));
        outer.insert("name".to_string(), Value::Text("Alice".to_string()));

        let subquery_fn = |_: &HashMap<String, Value>| {
            let mut row = HashMap::new();
            row.insert("value".to_string(), Value::Int(100));
            Ok(vec![row])
        };

        let results = LateralSubqueryExecutor::execute(vec![outer], subquery_fn).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(results[0].get("value"), Some(&Value::Int(100)));
    }
}
