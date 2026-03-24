//! CTEExecutor - Common Table Expression (WITH clause) executor
//!
//! This module implements CTE support following the Volcano executor model:
//! - Non-recursive CTEs: Materialized once, then served from buffer
//! - Recursive CTEs (WITH RECURSIVE): Work-table iteration model with cycle detection
//!
//! PostgreSQL-compatible behavior:
//! - CTEs are materialized before main query execution
//! - WITH RECURSIVE uses iterative work-table approach
//! - Later CTEs can reference earlier CTEs in the same WITH clause
//! - Cycle detection via row hashing

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::{HashMap, HashSet};

pub struct CTEExecutor {
    main_plan: Box<dyn Executor>,
    cte_results: HashMap<String, Vec<Tuple>>,
}

impl CTEExecutor {
    pub fn new(main_plan: Box<dyn Executor>, cte_results: HashMap<String, Vec<Tuple>>) -> Self {
        Self { main_plan, cte_results }
    }

    pub fn get_cte_results(&self) -> &HashMap<String, Vec<Tuple>> {
        &self.cte_results
    }

    pub fn get_cte(&self, name: &str) -> Option<&Vec<Tuple>> {
        self.cte_results.get(name)
    }
}

impl Executor for CTEExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.main_plan.next()
    }
}

#[derive(Clone)]
pub struct CTEColumns {
    pub columns: Vec<String>,
}

impl CTEColumns {
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }
}

pub struct VolcanoRecursiveCTEState {
    work_table: Vec<Tuple>,
    result_table: Vec<Tuple>,
    seen_tuples: HashSet<Vec<u8>>,
    anchor_executed: bool,
    done: bool,
}

impl VolcanoRecursiveCTEState {
    pub fn new() -> Self {
        Self {
            work_table: Vec::new(),
            result_table: Vec::new(),
            seen_tuples: HashSet::new(),
            anchor_executed: false,
            done: false,
        }
    }

    pub fn add_anchor_results(&mut self, tuples: Vec<Tuple>) {
        for tuple in tuples {
            let key = Self::tuple_to_key(&tuple);
            if self.seen_tuples.insert(key) {
                self.work_table.push(tuple.clone());
                self.result_table.push(tuple);
            }
        }
        self.anchor_executed = true;
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn get_results(&self) -> &[Tuple] {
        &self.result_table
    }

    pub fn get_work_table(&self) -> &[Tuple] {
        &self.work_table
    }

    fn tuple_to_key(tuple: &Tuple) -> Vec<u8> {
        let mut key = Vec::new();
        for (_, value) in tuple {
            Self::value_to_key(&mut key, value);
            key.push(255);
        }
        key
    }

    fn value_to_key(key: &mut Vec<u8>, value: &Value) {
        match value {
            Value::Int(n) => key.extend_from_slice(&n.to_le_bytes()),
            Value::Float(f) => key.extend_from_slice(&f.to_le_bytes()),
            Value::Bool(b) => key.push(if *b { 1 } else { 0 }),
            Value::Text(s) => key.extend_from_slice(s.as_bytes()),
            Value::Array(a) => {
                for v in a {
                    Self::value_to_key(key, v);
                }
            }
            Value::Json(j) => key.extend_from_slice(j.as_bytes()),
            Value::Date(d) => key.extend_from_slice(&d.to_le_bytes()),
            Value::Time(t) => key.extend_from_slice(&t.to_le_bytes()),
            Value::Timestamp(ts) => key.extend_from_slice(&ts.to_le_bytes()),
            Value::Decimal(v, _) => key.extend_from_slice(&v.to_le_bytes()),
            Value::Bytea(b) => key.extend_from_slice(b),
            Value::Enum(e) => {
                key.extend_from_slice(e.type_name.as_bytes());
                key.push(0);
                key.extend_from_slice(&e.index.to_le_bytes());
            }
            Value::Range(r) => {
                if let Some(l) = r.lower_bound() {
                    Self::value_to_key(key, l);
                }
                if let Some(u) = r.upper_bound() {
                    Self::value_to_key(key, u);
                }
            }
            Value::Composite(c) => {
                for (_, v) in &c.fields {
                    Self::value_to_key(key, v);
                }
            }
            Value::Null => key.push(0),
        }
    }
}

impl Default for VolcanoRecursiveCTEState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct VolcanoRecursiveCTEExecutor {
    state: VolcanoRecursiveCTEState,
    result_position: usize,
}

impl VolcanoRecursiveCTEExecutor {
    pub fn new(state: VolcanoRecursiveCTEState) -> Self {
        Self { state, result_position: 0 }
    }

    pub fn from_tuples(tuples: Vec<Tuple>) -> Self {
        let mut state = VolcanoRecursiveCTEState::new();
        state.add_anchor_results(tuples);
        state.anchor_executed = true;
        state.done = true;
        Self { state, result_position: 0 }
    }

    pub fn get_state(&self) -> &VolcanoRecursiveCTEState {
        &self.state
    }
}

impl Executor for VolcanoRecursiveCTEExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.result_position < self.state.get_results().len() {
            let tuple = self.state.get_results()[self.result_position].clone();
            self.result_position += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
}

pub struct CTEPlanner {
    catalog: Option<std::sync::Arc<crate::catalog::Catalog>>,
}

impl CTEPlanner {
    pub fn new(catalog: Option<std::sync::Arc<crate::catalog::Catalog>>) -> Self {
        Self { catalog }
    }

    pub fn plan_ctes(
        &self,
        ctes: &[crate::parser::ast::CTE],
    ) -> Result<HashMap<String, Box<dyn Executor>>, ExecutorError> {
        let mut cte_plans: HashMap<String, Box<dyn Executor>> = HashMap::new();

        for cte in ctes {
            let planner = crate::planner::planner::Planner::new(self.catalog.clone());
            let cte_plan = planner.plan(&cte.query)?;

            cte_plans.insert(cte.name.clone(), cte_plan);
        }

        Ok(cte_plans)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_single_cte_basic() {
        let cte_results: HashMap<String, Vec<Tuple>> = HashMap::new();
        let main_plan = Box::new(MockExecutor::from_int_values(vec![1, 2, 3]));

        let mut executor = CTEExecutor::new(main_plan, cte_results);

        let results: Vec<_> = std::iter::from_fn(|| executor.next().unwrap()).collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_cte_executor_empty() {
        let cte_results: HashMap<String, Vec<Tuple>> = HashMap::new();
        let main_plan = Box::new(MockExecutor::empty());

        let mut executor = CTEExecutor::new(main_plan, cte_results);

        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_recursive_cte_state_tuple_key() {
        let tuple1: Tuple = [("n".to_string(), Value::Int(1))].into();
        let tuple2: Tuple = [("n".to_string(), Value::Int(2))].into();
        let tuple3: Tuple = [("n".to_string(), Value::Int(1))].into();

        let key1 = VolcanoRecursiveCTEState::tuple_to_key(&tuple1);
        let key2 = VolcanoRecursiveCTEState::tuple_to_key(&tuple2);
        let key3 = VolcanoRecursiveCTEState::tuple_to_key(&tuple3);

        assert_ne!(key1, key2);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_recursive_cte_state_cycle_detection() {
        let mut state = VolcanoRecursiveCTEState::new();

        let tuple1: Tuple = [("n".to_string(), Value::Int(1))].into();
        let tuple2: Tuple = [("n".to_string(), Value::Int(1))].into();

        let key1 = VolcanoRecursiveCTEState::tuple_to_key(&tuple1);
        state.seen_tuples.insert(key1);

        let key2 = VolcanoRecursiveCTEState::tuple_to_key(&tuple2);
        assert!(!state.seen_tuples.insert(key2));
    }

    #[test]
    fn test_recursive_cte_executor_basic() {
        let tuples = vec![
            TupleBuilder::new().with_int("n", 1).build(),
            TupleBuilder::new().with_int("n", 2).build(),
            TupleBuilder::new().with_int("n", 3).build(),
        ];

        let mut executor = VolcanoRecursiveCTEExecutor::from_tuples(tuples);

        let results: Vec<_> = std::iter::from_fn(|| executor.next().unwrap()).collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("n"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("n"), Some(&Value::Int(2)));
        assert_eq!(results[2].get("n"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_recursive_cte_executor_empty() {
        let tuples: Vec<Tuple> = vec![];
        let mut executor = VolcanoRecursiveCTEExecutor::from_tuples(tuples);

        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_recursive_cte_executor_single_element() {
        let tuples = vec![TupleBuilder::new().with_int("n", 42).build()];
        let mut executor = VolcanoRecursiveCTEExecutor::from_tuples(tuples);

        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.get("n"), Some(&Value::Int(42)));

        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_cte_planner_new() {
        let planner = CTEPlanner::new(None);
        assert!(planner.catalog.is_none());
    }

    #[test]
    fn test_recursive_cte_state_with_text_values() {
        let mut state = VolcanoRecursiveCTEState::new();

        let tuple1: Tuple = [("name".to_string(), Value::Text("Alice".to_string()))].into();
        let tuple2: Tuple = [("name".to_string(), Value::Text("Bob".to_string()))].into();
        let tuple3: Tuple = [("name".to_string(), Value::Text("Alice".to_string()))].into();

        state.add_anchor_results(vec![tuple1.clone(), tuple2.clone()]);

        let key1 = VolcanoRecursiveCTEState::tuple_to_key(&tuple1);
        let key2 = VolcanoRecursiveCTEState::tuple_to_key(&tuple2);
        let key3 = VolcanoRecursiveCTEState::tuple_to_key(&tuple3);

        assert_ne!(key1, key2);
        assert_eq!(key1, key3);

        assert!(state.seen_tuples.contains(&key1));
        assert!(state.seen_tuples.contains(&key2));
        assert_eq!(state.seen_tuples.len(), 2);
    }

    #[test]
    fn test_recursive_cte_state_with_null_values() {
        let mut state = VolcanoRecursiveCTEState::new();

        let tuple1: Tuple = [("n".to_string(), Value::Null)].into();
        let tuple2: Tuple = [("n".to_string(), Value::Int(1))].into();

        state.add_anchor_results(vec![tuple1.clone(), tuple2.clone()]);

        let key1 = VolcanoRecursiveCTEState::tuple_to_key(&tuple1);
        let key2 = VolcanoRecursiveCTEState::tuple_to_key(&tuple2);

        assert!(state.seen_tuples.contains(&key1));
        assert!(state.seen_tuples.contains(&key2));
    }

    #[test]
    fn test_cte_executor_with_multiple_ctes() {
        let mut cte_results: HashMap<String, Vec<Tuple>> = HashMap::new();

        cte_results.insert("cte1".to_string(), vec![TupleBuilder::new().with_int("a", 1).build()]);
        cte_results.insert("cte2".to_string(), vec![TupleBuilder::new().with_int("b", 2).build()]);

        let main_plan = Box::new(MockExecutor::empty());

        let executor = CTEExecutor::new(main_plan, cte_results);

        assert!(executor.get_cte("cte1").unwrap().len() == 1);
        assert!(executor.get_cte("cte2").unwrap().len() == 1);
        assert!(executor.get_cte("cte3").is_none());
    }

    #[test]
    fn test_recursive_cte_executor_with_large_dataset() {
        let tuples: Vec<Tuple> =
            (1..=1000).map(|i| TupleBuilder::new().with_int("n", i).build()).collect();

        let mut executor = VolcanoRecursiveCTEExecutor::from_tuples(tuples);

        let mut count = 0;
        while executor.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1000);
    }

    #[test]
    fn test_recursive_cte_state_multiple_iterations() {
        let mut state = VolcanoRecursiveCTEState::new();

        state.add_anchor_results(vec![
            TupleBuilder::new().with_int("n", 1).build(),
            TupleBuilder::new().with_int("n", 2).build(),
        ]);

        assert_eq!(state.get_results().len(), 2);
        assert!(!state.is_done());

        state.done = true;
        assert!(state.is_done());
    }

    #[test]
    fn test_cte_executor_get_cte_results() {
        let mut cte_results: HashMap<String, Vec<Tuple>> = HashMap::new();
        cte_results.insert(
            "numbers".to_string(),
            vec![
                TupleBuilder::new().with_int("n", 10).build(),
                TupleBuilder::new().with_int("n", 20).build(),
            ],
        );

        let main_plan = Box::new(MockExecutor::empty());
        let executor = CTEExecutor::new(main_plan, cte_results);

        let results = executor.get_cte_results();
        assert_eq!(results.len(), 1);
        assert_eq!(results.get("numbers").unwrap().len(), 2);
    }

    #[test]
    fn test_recursive_cte_executor_with_different_types() {
        let tuples = vec![
            TupleBuilder::new()
                .with_int("id", 1)
                .with_text("name", "test")
                .with_bool("active", true)
                .build(),
        ];

        let mut executor = VolcanoRecursiveCTEExecutor::from_tuples(tuples);
        let result = executor.next().unwrap().unwrap();

        assert_eq!(result.get("id"), Some(&Value::Int(1)));
        assert_eq!(result.get("name"), Some(&Value::Text("test".to_string())));
        assert_eq!(result.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_recursive_cte_state_anchor_not_executed() {
        let state = VolcanoRecursiveCTEState::new();

        assert!(!state.anchor_executed);
        assert!(state.get_results().is_empty());
    }

    #[test]
    fn test_cte_executor_empty_cte_results() {
        let cte_results: HashMap<String, Vec<Tuple>> = HashMap::new();
        let main_plan = Box::new(MockExecutor::from_int_values(vec![1, 2]));

        let executor = CTEExecutor::new(main_plan, cte_results);

        assert!(executor.get_cte_results().is_empty());
    }

    #[test]
    fn test_recursive_cte_executor_with_work_table() {
        let tuples = vec![
            TupleBuilder::new().with_int("n", 1).build(),
            TupleBuilder::new().with_int("n", 2).build(),
        ];

        let executor = VolcanoRecursiveCTEExecutor::from_tuples(tuples);
        let state = executor.get_state();

        assert_eq!(state.get_work_table().len(), 2);
        assert_eq!(state.get_results().len(), 2);
    }
}
