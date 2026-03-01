use crate::catalog::Value;
use std::collections::HashMap;

pub struct MultipleCTEExecutor {
    ctes: HashMap<String, Vec<HashMap<String, Value>>>,
}

impl MultipleCTEExecutor {
    pub fn new() -> Self {
        Self { ctes: HashMap::new() }
    }

    pub fn add_cte(&mut self, name: String, results: Vec<HashMap<String, Value>>) {
        self.ctes.insert(name, results);
    }

    pub fn get_cte(&self, name: &str) -> Option<&Vec<HashMap<String, Value>>> {
        self.ctes.get(name)
    }

    pub fn execute_with_ctes<F>(&self, query_fn: F) -> Result<Vec<HashMap<String, Value>>, String>
    where
        F: Fn(
            &HashMap<String, Vec<HashMap<String, Value>>>,
        ) -> Result<Vec<HashMap<String, Value>>, String>,
    {
        query_fn(&self.ctes)
    }
}

impl Default for MultipleCTEExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_cte() {
        let mut executor = MultipleCTEExecutor::new();

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Int(1));
        executor.add_cte("cte1".to_string(), vec![row]);

        let result = executor.get_cte("cte1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_ctes() {
        let mut executor = MultipleCTEExecutor::new();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Int(1));
        executor.add_cte("cte1".to_string(), vec![row1]);

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), Value::Text("Alice".to_string()));
        executor.add_cte("cte2".to_string(), vec![row2]);

        assert!(executor.get_cte("cte1").is_some());
        assert!(executor.get_cte("cte2").is_some());
        assert!(executor.get_cte("cte3").is_none());
    }

    #[test]
    fn test_execute_with_ctes() {
        let mut executor = MultipleCTEExecutor::new();

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Int(1));
        executor.add_cte("cte1".to_string(), vec![row]);

        let result = executor
            .execute_with_ctes(|ctes| {
                let cte1 = ctes.get("cte1").unwrap();
                Ok(cte1.clone())
            })
            .unwrap();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_empty_executor() {
        let executor = MultipleCTEExecutor::new();
        assert!(executor.get_cte("nonexistent").is_none());
    }
}
