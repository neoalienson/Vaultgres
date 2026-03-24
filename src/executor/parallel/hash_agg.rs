//! ParallelHashAgg - Parallel hash-based aggregation with GROUP BY
//!
//! Follows PostgreSQL's parallel aggregation design:
//! - Partial phase: Each worker performs local aggregation
//! - Final phase: Leader combines results from all workers
//! - Uses hash partitioning to distribute groups across workers

use crate::catalog::{TableSchema, Value};
use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::partition::PartitionStrategy;
use crate::parser::ast::{AggregateFunc, Expr};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum(i64),
    Avg { sum: i64, count: i64 },
    Min(Value),
    Max(Value),
}

impl AggregateState {
    fn new(func: &AggregateFunc) -> Self {
        match func {
            AggregateFunc::Count => AggregateState::Count(0),
            AggregateFunc::Sum => AggregateState::Sum(0),
            AggregateFunc::Avg => AggregateState::Avg { sum: 0, count: 0 },
            AggregateFunc::Min => AggregateState::Min(Value::Null),
            AggregateFunc::Max => AggregateState::Max(Value::Null),
        }
    }

    fn update(&mut self, value: &Value) {
        match self {
            AggregateState::Count(c) => {
                if !matches!(value, Value::Null) {
                    *c += 1;
                }
            }
            AggregateState::Sum(s) => {
                if let Value::Int(v) = value {
                    *s += v;
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Value::Int(v) = value {
                    *sum += v;
                    *count += 1;
                }
            }
            AggregateState::Min(current_min) => {
                if matches!(current_min, Value::Null)
                    || Self::compare_values(value, current_min).unwrap_or(std::cmp::Ordering::Less)
                        == std::cmp::Ordering::Less
                {
                    *current_min = value.clone();
                }
            }
            AggregateState::Max(current_max) => {
                if matches!(current_max, Value::Null)
                    || Self::compare_values(value, current_max)
                        .unwrap_or(std::cmp::Ordering::Greater)
                        == std::cmp::Ordering::Greater
                {
                    *current_max = value.clone();
                }
            }
        }
    }

    fn merge(&mut self, other: &AggregateState) {
        match (self, other) {
            (AggregateState::Count(a), AggregateState::Count(b)) => *a += *b,
            (AggregateState::Sum(a), AggregateState::Sum(b)) => *a += *b,
            (
                AggregateState::Avg { sum: a, count: ca },
                AggregateState::Avg { sum: b, count: cb },
            ) => {
                *a += *b;
                *ca += *cb;
            }
            (AggregateState::Min(a), AggregateState::Min(b)) => {
                if !matches!(b, Value::Null) {
                    if matches!(a, Value::Null)
                        || Self::compare_values(b, a).unwrap_or(std::cmp::Ordering::Less)
                            == std::cmp::Ordering::Less
                    {
                        *a = b.clone();
                    }
                }
            }
            (AggregateState::Max(a), AggregateState::Max(b)) => {
                if !matches!(b, Value::Null) {
                    if matches!(a, Value::Null)
                        || Self::compare_values(b, a).unwrap_or(std::cmp::Ordering::Greater)
                            == std::cmp::Ordering::Greater
                    {
                        *a = b.clone();
                    }
                }
            }
            _ => {}
        }
    }

    fn finalize(&self) -> Value {
        match self {
            AggregateState::Count(c) => Value::Int(*c),
            AggregateState::Sum(s) => Value::Int(*s),
            AggregateState::Avg { sum, count } => {
                if *count > 0 {
                    Value::Int(*sum / *count)
                } else {
                    Value::Null
                }
            }
            AggregateState::Min(v) => v.clone(),
            AggregateState::Max(v) => v.clone(),
        }
    }

    fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
            (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
            (Value::Null, _) => Ok(std::cmp::Ordering::Less),
            (_, Value::Null) => Ok(std::cmp::Ordering::Greater),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare different types".to_string())),
        }
    }
}

#[derive(Clone)]
struct GroupState {
    group_tuple: Tuple,
    agg_states: Vec<AggregateState>,
}

impl GroupState {
    fn new(tuple: &Tuple, group_by: &[Expr], aggregates: &[Expr]) -> Self {
        let mut group_tuple = Tuple::new();
        for expr in group_by {
            match expr {
                Expr::Column(name) => {
                    if let Some(val) = tuple.get(name) {
                        group_tuple.insert(name.clone(), val.clone());
                    }
                }
                Expr::QualifiedColumn { table, column } => {
                    let qualified_name = format!("{}.{}", table, column);
                    if let Some(val) = tuple.get(&qualified_name).or_else(|| tuple.get(column)) {
                        group_tuple.insert(qualified_name, val.clone());
                    }
                }
                _ => {}
            }
        }

        let agg_states: Vec<AggregateState> = aggregates
            .iter()
            .map(|agg_expr| {
                if let Expr::Aggregate { func, .. } = agg_expr {
                    AggregateState::new(func)
                } else {
                    AggregateState::new(&AggregateFunc::Count)
                }
            })
            .collect();

        Self { group_tuple, agg_states }
    }

    fn merge_with(&mut self, other: &GroupState) {
        for (i, agg_state) in self.agg_states.iter_mut().enumerate() {
            agg_state.merge(&other.agg_states[i]);
        }
    }
}

pub struct ParallelHashAgg {
    child: Arc<dyn ParallelOperator>,
    group_by: Vec<Expr>,
    aggregates: Vec<Expr>,
    output_schema: TableSchema,
    partition_strategy: Arc<PartitionStrategy>,
    hash_tables: Vec<Mutex<HashMap<Vec<u8>, GroupState>>>,
}

impl ParallelHashAgg {
    pub fn new(
        child: Arc<dyn ParallelOperator>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
        num_partitions: usize,
    ) -> Self {
        let partition_strategy = Arc::new(PartitionStrategy::new(num_partitions));
        let hash_tables = (0..num_partitions).map(|_| Mutex::new(HashMap::new())).collect();

        Self { child, group_by, aggregates, output_schema, partition_strategy, hash_tables }
    }

    pub fn with_default_partitions(
        child: Arc<dyn ParallelOperator>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
    ) -> Self {
        Self::new(
            child,
            group_by,
            aggregates,
            output_schema,
            PartitionStrategy::optimal_partitions(4),
        )
    }

    fn compute_group_key(&self, tuple: &Tuple) -> Result<Vec<u8>, ExecutorError> {
        let mut hasher = DefaultHasher::new();
        for expr in &self.group_by {
            match expr {
                Expr::Column(name) => {
                    if let Some(val) = tuple.get(name) {
                        Self::hash_value(val, &mut hasher);
                    }
                }
                Expr::QualifiedColumn { table, column } => {
                    let qualified_name = format!("{}.{}", table, column);
                    if let Some(val) = tuple.get(&qualified_name).or_else(|| tuple.get(column)) {
                        Self::hash_value(val, &mut hasher);
                    }
                }
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported GROUP BY expression: {:?}",
                        expr
                    )));
                }
            }
        }
        Ok(hasher.finish().to_le_bytes().to_vec())
    }

    fn hash_value(value: &Value, hasher: &mut DefaultHasher) {
        match value {
            Value::Int(n) => {
                "int".hash(hasher);
                n.hash(hasher);
            }
            Value::Text(s) => {
                "text".hash(hasher);
                s.hash(hasher);
            }
            Value::Bool(b) => {
                "bool".hash(hasher);
                b.hash(hasher);
            }
            Value::Null => {
                "null".hash(hasher);
            }
            _ => {
                format!("{:?}", value).hash(hasher);
            }
        }
    }

    fn partition_key(&self, key: &[u8]) -> usize {
        self.partition_strategy.partition_key(key)
    }

    pub fn local_aggregate(&self, morsel: Morsel) -> Result<(), ExecutorError> {
        for tuple in morsel.tuples {
            let group_key = self.compute_group_key(&tuple)?;
            let partition_id = self.partition_key(&group_key);
            let mut hash_table = self.hash_tables[partition_id].lock().unwrap();

            let entry = hash_table
                .entry(group_key)
                .or_insert_with(|| GroupState::new(&tuple, &self.group_by, &self.aggregates));

            for (i, agg_expr) in self.aggregates.iter().enumerate() {
                if let Expr::Aggregate { func: _, arg } = agg_expr {
                    let arg_val = if matches!(arg.as_ref(), Expr::Star) {
                        Value::Int(1)
                    } else {
                        Self::eval_aggregate_arg(arg.as_ref(), &tuple)?
                    };
                    entry.agg_states[i].update(&arg_val);
                }
            }
        }
        Ok(())
    }

    fn eval_aggregate_arg(arg: &Expr, tuple: &Tuple) -> Result<Value, ExecutorError> {
        match arg {
            Expr::Column(name) => tuple
                .get(name)
                .cloned()
                .ok_or_else(|| ExecutorError::InternalError("Column not found".to_string())),
            Expr::QualifiedColumn { table, column } => {
                let qualified_name = format!("{}.{}", table, column);
                tuple
                    .get(&qualified_name)
                    .or_else(|| tuple.get(column))
                    .cloned()
                    .ok_or_else(|| ExecutorError::InternalError("Column not found".to_string()))
            }
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unsupported aggregate argument: {:?}",
                arg
            ))),
        }
    }

    pub fn global_combine(&self) -> Result<Vec<Tuple>, ExecutorError> {
        let mut global_groups: HashMap<Vec<u8>, GroupState> = HashMap::new();

        for partition in &self.hash_tables {
            let guard = partition.lock().unwrap();
            for (key, state) in guard.iter() {
                global_groups
                    .entry(key.clone())
                    .and_modify(|existing| existing.merge_with(state))
                    .or_insert_with(|| state.clone());
            }
        }

        let mut results = Vec::new();

        if self.group_by.is_empty() && global_groups.is_empty() {
            let mut group_tuple = Tuple::new();
            for agg_expr in &self.aggregates {
                let agg_name = Self::get_aggregate_name(agg_expr);
                let agg_value = if let Expr::Aggregate { func, .. } = agg_expr {
                    match func {
                        AggregateFunc::Count => Value::Int(0),
                        AggregateFunc::Sum => Value::Int(0),
                        AggregateFunc::Avg => Value::Null,
                        AggregateFunc::Min => Value::Null,
                        AggregateFunc::Max => Value::Null,
                    }
                } else {
                    Value::Null
                };
                group_tuple.insert(agg_name, agg_value);
            }
            results.push(group_tuple);
            return Ok(results);
        }

        for (_, group_state) in global_groups {
            let mut result_tuple = group_state.group_tuple;
            for (i, agg_expr) in self.aggregates.iter().enumerate() {
                let agg_name = Self::get_aggregate_name(agg_expr);
                let agg_value = group_state.agg_states[i].finalize();
                result_tuple.insert(agg_name, agg_value);
            }
            results.push(result_tuple);
        }

        Ok(results)
    }

    fn get_aggregate_name(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate { func, arg } => {
                let arg_name = match arg.as_ref() {
                    Expr::Column(col_name) => col_name.clone(),
                    Expr::QualifiedColumn { column, .. } => column.clone(),
                    Expr::Star => "*".to_string(),
                    _ => "expr".to_string(),
                };
                format!("{:?}({})", func, arg_name).to_lowercase()
            }
            Expr::Alias { alias, .. } => alias.clone(),
            _ => format!("{:?}", expr),
        }
    }

    pub fn execute(&self, config: &ParallelConfig) -> Result<Vec<Tuple>, ExecutorError> {
        let mut morsel_results = Vec::new();

        let num_workers = config.max_workers().max(1);
        let rows_per_worker = (self.child.degree_of_parallelism() / num_workers).max(1);

        let mut offset = 0;
        while offset < self.child.degree_of_parallelism() {
            let end = (offset + rows_per_worker).min(self.child.degree_of_parallelism());
            let morsel =
                Morsel { tuples: vec![], start_offset: offset, end_offset: end, partition_id: 0 };

            let child_result = self.child.process_morsel(morsel)?;
            morsel_results.push(child_result);
            offset = end;
        }

        for morsel in morsel_results {
            self.local_aggregate(morsel)?;
        }

        self.global_combine()
    }
}

impl ParallelOperator for ParallelHashAgg {
    fn process_morsel(&self, morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let child_morsel = self.child.process_morsel(morsel)?;
        self.local_aggregate(child_morsel)?;
        Ok(Morsel { tuples: vec![], start_offset: 0, end_offset: 0, partition_id: 0 })
    }

    fn degree_of_parallelism(&self) -> usize {
        self.hash_tables.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableSchema;
    use crate::executor::test_helpers::TupleBuilder;

    struct MockOperator {
        tuples: Vec<Tuple>,
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    fn create_test_tuples() -> Vec<Tuple> {
        vec![
            TupleBuilder::new().with_int("category", 1).with_int("value", 10).build(),
            TupleBuilder::new().with_int("category", 1).with_int("value", 20).build(),
            TupleBuilder::new().with_int("category", 2).with_int("value", 30).build(),
        ]
    }

    fn create_aggregates() -> Vec<Expr> {
        vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        }]
    }

    fn create_group_by() -> Vec<Expr> {
        vec![Expr::Column("category".to_string())]
    }

    #[test]
    fn test_aggregate_state_count() {
        let mut state = AggregateState::new(&AggregateFunc::Count);
        state.update(&Value::Int(1));
        state.update(&Value::Int(2));
        state.update(&Value::Null);

        assert!(matches!(state, AggregateState::Count(2)));
    }

    #[test]
    fn test_aggregate_state_sum() {
        let mut state = AggregateState::new(&AggregateFunc::Sum);
        state.update(&Value::Int(10));
        state.update(&Value::Int(20));
        state.update(&Value::Null);

        assert!(matches!(state, AggregateState::Sum(30)));
    }

    #[test]
    fn test_aggregate_state_avg() {
        let mut state = AggregateState::new(&AggregateFunc::Avg);
        state.update(&Value::Int(10));
        state.update(&Value::Int(20));
        state.update(&Value::Null);

        if let AggregateState::Avg { sum, count } = state {
            assert_eq!(sum, 30);
            assert_eq!(count, 2);
        } else {
            panic!("Expected Avg state");
        }
    }

    #[test]
    fn test_aggregate_state_min_max() {
        let mut min_state = AggregateState::new(&AggregateFunc::Min);
        let mut max_state = AggregateState::new(&AggregateFunc::Max);

        min_state.update(&Value::Int(10));
        min_state.update(&Value::Int(5));
        min_state.update(&Value::Int(20));

        max_state.update(&Value::Int(10));
        max_state.update(&Value::Int(5));
        max_state.update(&Value::Int(20));

        assert!(matches!(min_state, AggregateState::Min(Value::Int(5))));
        assert!(matches!(max_state, AggregateState::Max(Value::Int(20))));
    }

    #[test]
    fn test_aggregate_state_merge() {
        let mut state1 = AggregateState::new(&AggregateFunc::Sum);
        state1.update(&Value::Int(10));

        let mut state2 = AggregateState::new(&AggregateFunc::Sum);
        state2.update(&Value::Int(20));

        state1.merge(&state2);
        assert!(matches!(state1, AggregateState::Sum(30)));
    }

    #[test]
    fn test_aggregate_state_count_merge() {
        let mut state1 = AggregateState::new(&AggregateFunc::Count);
        state1.update(&Value::Int(1));
        state1.update(&Value::Int(2));

        let mut state2 = AggregateState::new(&AggregateFunc::Count);
        state2.update(&Value::Int(3));
        state2.update(&Value::Null);

        state1.merge(&state2);
        assert!(matches!(state1, AggregateState::Count(3)));
    }

    #[test]
    fn test_aggregate_state_finalize() {
        let mut state = AggregateState::new(&AggregateFunc::Avg);
        state.update(&Value::Int(10));
        state.update(&Value::Int(20));

        let result = state.finalize();
        assert_eq!(result, Value::Int(15));
    }

    #[test]
    fn test_aggregate_state_finalize_null() {
        let state = AggregateState::new(&AggregateFunc::Avg);
        let result = state.finalize();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_parallel_hash_agg_basic() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, vec![], aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("sum(value)"), Some(&Value::Int(60)));
    }

    #[test]
    fn test_parallel_hash_agg_group_by() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 2);

        let cat1_result =
            results.iter().find(|t| t.get("category") == Some(&Value::Int(1))).unwrap();
        assert_eq!(cat1_result.get("sum(value)"), Some(&Value::Int(30)));

        let cat2_result =
            results.iter().find(|t| t.get("category") == Some(&Value::Int(2))).unwrap();
        assert_eq!(cat2_result.get("sum(value)"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_parallel_hash_agg_multiple_aggregates() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Count,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("value".to_string())),
            },
        ];
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 2);

        let cat1_result =
            results.iter().find(|t| t.get("category") == Some(&Value::Int(1))).unwrap();
        assert_eq!(cat1_result.get("sum(value)"), Some(&Value::Int(30)));
        assert_eq!(cat1_result.get("count(value)"), Some(&Value::Int(2)));
        assert_eq!(cat1_result.get("min(value)"), Some(&Value::Int(10)));
        assert_eq!(cat1_result.get("max(value)"), Some(&Value::Int(20)));
    }

    #[test]
    fn test_parallel_hash_agg_multiple_columns() {
        let tuples = vec![
            TupleBuilder::new()
                .with_int("region", 1)
                .with_int("category", 1)
                .with_int("value", 10)
                .build(),
            TupleBuilder::new()
                .with_int("region", 1)
                .with_int("category", 2)
                .with_int("value", 20)
                .build(),
            TupleBuilder::new()
                .with_int("region", 2)
                .with_int("category", 1)
                .with_int("value", 30)
                .build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let group_by =
            vec![Expr::Column("region".to_string()), Expr::Column("category".to_string())];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 3);

        let region1_cat1 = results
            .iter()
            .find(|t| {
                t.get("region") == Some(&Value::Int(1)) && t.get("category") == Some(&Value::Int(1))
            })
            .unwrap();
        assert_eq!(region1_cat1.get("sum(value)"), Some(&Value::Int(10)));
    }

    #[test]
    fn test_parallel_hash_agg_empty_input() {
        let tuples: Vec<Tuple> = vec![];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, vec![], aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 0, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("sum(value)"), Some(&Value::Int(0)));
    }

    #[test]
    fn test_parallel_hash_agg_empty_input_with_group_by() {
        let tuples: Vec<Tuple> = vec![];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 0, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parallel_hash_agg_count_star() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let aggregates =
            vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 2);

        let cat1 = results.iter().find(|t| t.get("category") == Some(&Value::Int(1))).unwrap();
        assert_eq!(cat1.get("count(*)"), Some(&Value::Int(2)));

        let cat2 = results.iter().find(|t| t.get("category") == Some(&Value::Int(2))).unwrap();
        assert_eq!(cat2.get("count(*)"), Some(&Value::Int(1)));
    }

    #[test]
    fn test_parallel_hash_agg_null_handling() {
        let tuples = vec![
            TupleBuilder::new().with_int("category", 1).with_value("value", Value::Null).build(),
            TupleBuilder::new().with_int("category", 1).with_int("value", 20).build(),
            TupleBuilder::new().with_int("category", 1).with_value("value", Value::Null).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunc::Count,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("value".to_string())),
            },
        ];
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 1);

        let result = &results[0];
        assert_eq!(result.get("count(value)"), Some(&Value::Int(1)));
        assert_eq!(result.get("sum(value)"), Some(&Value::Int(20)));
    }

    #[test]
    fn test_parallel_hash_agg_text_key() {
        let tuples = vec![
            TupleBuilder::new().with_text("category", "a").with_int("value", 10).build(),
            TupleBuilder::new().with_text("category", "a").with_int("value", 20).build(),
            TupleBuilder::new().with_text("category", "b").with_int("value", 30).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let group_by = vec![Expr::Column("category".to_string())];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 2);

        let cat_a = results
            .iter()
            .find(|t| t.get("category") == Some(&Value::Text("a".to_string())))
            .unwrap();
        assert_eq!(cat_a.get("sum(value)"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_parallel_hash_agg_bool_key() {
        let tuples = vec![
            TupleBuilder::new().with_bool("active", true).with_int("value", 10).build(),
            TupleBuilder::new().with_bool("active", true).with_int("value", 20).build(),
            TupleBuilder::new().with_bool("active", false).with_int("value", 30).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let group_by = vec![Expr::Column("active".to_string())];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 2);

        let true_result =
            results.iter().find(|t| t.get("active") == Some(&Value::Bool(true))).unwrap();
        assert_eq!(true_result.get("sum(value)"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_parallel_hash_agg_partition_aware() {
        let tuples: Vec<Tuple> = (0..100)
            .map(|i| TupleBuilder::new().with_int("category", i % 10).with_int("value", i).build())
            .collect();
        let child = Arc::new(MockOperator { tuples });
        let aggregates = create_aggregates();
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let num_partitions = 8;
        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, num_partitions);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 100, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 10);

        let mut total: i64 = 0;
        for r in &results {
            if let Value::Int(v) = r.get("sum(value)").unwrap() {
                total += *v;
            }
        }
        assert_eq!(total, (0..100).sum::<i64>());
    }

    #[test]
    fn test_parallel_hash_agg_avg() {
        let tuples = vec![
            TupleBuilder::new().with_int("category", 1).with_int("value", 10).build(),
            TupleBuilder::new().with_int("category", 1).with_int("value", 20).build(),
            TupleBuilder::new().with_int("category", 1).with_int("value", 30).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Avg,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        let group_by = create_group_by();
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("avg(value)"), Some(&Value::Int(20)));
    }

    #[test]
    fn test_parallel_hash_agg_single_group() {
        let tuples = vec![
            TupleBuilder::new().with_int("value", 10).build(),
            TupleBuilder::new().with_int("value", 20).build(),
            TupleBuilder::new().with_int("value", 30).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let aggregates = vec![
            Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) },
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Avg,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("value".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("value".to_string())),
            },
        ];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);

        let agg = ParallelHashAgg::new(child, vec![], aggregates, output_schema, 4);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.process_morsel(morsel).unwrap();
        let results = agg.global_combine().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("count(*)"), Some(&Value::Int(3)));
        assert_eq!(results[0].get("sum(value)"), Some(&Value::Int(60)));
        assert_eq!(results[0].get("avg(value)"), Some(&Value::Int(20)));
        assert_eq!(results[0].get("min(value)"), Some(&Value::Int(10)));
        assert_eq!(results[0].get("max(value)"), Some(&Value::Int(30)));
    }
}
