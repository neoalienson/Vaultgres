use crate::catalog::Value;
use std::collections::HashMap;

pub struct DerivedTableExecutor {
    rows: Vec<HashMap<String, Value>>,
    alias: String,
}

impl DerivedTableExecutor {
    pub fn new(rows: Vec<HashMap<String, Value>>, alias: String) -> Self {
        Self { rows, alias }
    }

    pub fn execute(&self) -> Vec<HashMap<String, Value>> {
        self.rows.clone()
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derived_table() {
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Int(1));
        row1.insert("name".to_string(), Value::Text("Alice".to_string()));

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Int(2));
        row2.insert("name".to_string(), Value::Text("Bob".to_string()));

        let executor = DerivedTableExecutor::new(vec![row1.clone(), row2.clone()], "t".to_string());
        let results = executor.execute();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(executor.alias(), "t");
    }

    #[test]
    fn test_empty_derived_table() {
        let executor = DerivedTableExecutor::new(vec![], "t".to_string());
        let results = executor.execute();
        assert_eq!(results.len(), 0);
    }
}
