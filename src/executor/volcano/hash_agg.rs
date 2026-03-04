//! HashAggExecutor - Performs hash-based aggregation (GROUP BY and aggregates)

use crate::catalog::{TableSchema, Value};
use crate::executor::eval::Eval;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::{AggregateFunc, Expr};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Aggregate state for tracking aggregation progress
#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum(i64),
    Avg { sum: i64, count: i64 },
    Min(Value),
    Max(Value),
}

pub struct HashAggExecutor {
    buffered_results: Vec<Tuple>,
    current_idx: usize,
    output_schema: TableSchema,
}

impl HashAggExecutor {
    pub fn new(
        mut child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
    ) -> Result<Self, ExecutorError> {
        // Group tuples by group_by keys
        let mut groups: HashMap<u64, (Tuple, Vec<AggregateState>)> = HashMap::new();

        while let Some(tuple) = child.next()? {
            // Compute group key hash
            let group_key = if group_by.is_empty() {
                // No GROUP BY - all tuples go into one group
                0
            } else {
                Self::compute_group_key(&tuple, &group_by)?
            };

            // Initialize or update group
            let entry = groups.entry(group_key).or_insert_with(|| {
                let mut group_tuple = Tuple::new();
                // Initialize group-by columns
                for expr in &group_by {
                    if let Expr::Column(col_name) = expr {
                        if let Some(val) = tuple.get(col_name) {
                            group_tuple.insert(col_name.clone(), val.clone());
                        }
                    }
                }
                // Initialize aggregate states based on the aggregate function type
                let agg_states: Vec<AggregateState> = aggregates
                    .iter()
                    .map(|agg_expr| {
                        if let Expr::Aggregate { func, .. } = agg_expr {
                            match func {
                                AggregateFunc::Count => AggregateState::Count(0),
                                AggregateFunc::Sum => AggregateState::Sum(0),
                                AggregateFunc::Avg => AggregateState::Avg { sum: 0, count: 0 },
                                AggregateFunc::Min => AggregateState::Min(Value::Null),
                                AggregateFunc::Max => AggregateState::Max(Value::Null),
                            }
                        } else {
                            AggregateState::Count(0)
                        }
                    })
                    .collect();
                (group_tuple, agg_states)
            });

            // Update aggregate states
            for (i, agg_expr) in aggregates.iter().enumerate() {
                if let Expr::Aggregate { func, arg } = agg_expr {
                    let arg_val = Eval::eval_expr(arg, &tuple)?;

                    match &mut entry.1[i] {
                        AggregateState::Count(c) => {
                            if !matches!(arg_val, Value::Null) {
                                *c += 1;
                            }
                        }
                        AggregateState::Sum(s) => {
                            if let Value::Int(v) = arg_val {
                                *s += v;
                            }
                        }
                        AggregateState::Avg { sum, count } => {
                            if let Value::Int(v) = arg_val {
                                *sum += v;
                                *count += 1;
                            }
                        }
                        AggregateState::Min(current_min) => {
                            if matches!(current_min, Value::Null)
                                || Self::compare_values(&arg_val, current_min)?
                                    == std::cmp::Ordering::Less
                            {
                                entry.1[i] = AggregateState::Min(arg_val);
                            }
                        }
                        AggregateState::Max(current_max) => {
                            if matches!(current_max, Value::Null)
                                || Self::compare_values(&arg_val, current_max)?
                                    == std::cmp::Ordering::Greater
                            {
                                entry.1[i] = AggregateState::Max(arg_val);
                            }
                        }
                    }
                }
            }
        }

        // Convert groups to result tuples
        let mut buffered_results = Vec::new();

        // If no GROUP BY and no input tuples, still return one row with aggregates (SQL standard behavior)
        if group_by.is_empty() && groups.is_empty() {
            let mut group_tuple = Tuple::new();
            // Initialize aggregate states for empty input
            let agg_states: Vec<AggregateState> = aggregates
                .iter()
                .map(|agg_expr| {
                    if let Expr::Aggregate { func, .. } = agg_expr {
                        match func {
                            AggregateFunc::Count => AggregateState::Count(0),
                            AggregateFunc::Sum => AggregateState::Sum(0),
                            AggregateFunc::Avg => AggregateState::Avg { sum: 0, count: 0 },
                            AggregateFunc::Min => AggregateState::Min(Value::Null),
                            AggregateFunc::Max => AggregateState::Max(Value::Null),
                        }
                    } else {
                        AggregateState::Count(0)
                    }
                })
                .collect();

            // Add aggregate results to the tuple
            for (i, agg_expr) in aggregates.iter().enumerate() {
                let agg_name = Self::get_aggregate_name(agg_expr);
                let agg_value = match &agg_states[i] {
                    AggregateState::Count(c) => Value::Int(*c),
                    AggregateState::Sum(s) => Value::Int(*s),
                    AggregateState::Avg { sum: _, count } => {
                        // For empty input, AVG returns NULL
                        Value::Null
                    }
                    AggregateState::Min(v) => v.clone(),
                    AggregateState::Max(v) => v.clone(),
                };
                group_tuple.insert(agg_name, agg_value);
            }
            buffered_results.push(group_tuple);
        } else {
            // Normal case: convert groups to result tuples
            for (_, (mut group_tuple, agg_states)) in groups {
                // Add aggregate results to the tuple
                for (i, agg_expr) in aggregates.iter().enumerate() {
                    let agg_name = Self::get_aggregate_name(agg_expr);
                    let agg_value = match &agg_states[i] {
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
                    };
                    group_tuple.insert(agg_name, agg_value);
                }
                buffered_results.push(group_tuple);
            }
        }

        Ok(Self { buffered_results, current_idx: 0, output_schema })
    }

    /// Compute a hash key for a group based on group-by expressions
    fn compute_group_key(tuple: &Tuple, group_by: &[Expr]) -> Result<u64, ExecutorError> {
        let mut hasher = DefaultHasher::new();
        for expr in group_by {
            if let Expr::Column(col_name) = expr {
                if let Some(val) = tuple.get(col_name) {
                    Self::hash_value(val, &mut hasher);
                }
            }
        }
        Ok(hasher.finish())
    }

    /// Hash a value
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

    /// Compare two values
    fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
            (Value::Null, _) => Ok(std::cmp::Ordering::Less),
            (_, Value::Null) => Ok(std::cmp::Ordering::Greater),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare different types".to_string())),
        }
    }

    /// Get a name for an aggregate expression
    fn get_aggregate_name(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate { func, arg } => {
                if let Expr::Column(col_name) = arg.as_ref() {
                    format!("{:?}({})", func, col_name).to_lowercase()
                } else {
                    format!("{:?}(expr)", func).to_lowercase()
                }
            }
            Expr::Alias { alias, .. } => alias.clone(),
            _ => format!("{:?}", expr),
        }
    }
}

impl Executor for HashAggExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.current_idx >= self.buffered_results.len() {
            return Ok(None);
        }

        let tuple = self.buffered_results[self.current_idx].clone();
        self.current_idx += 1;
        Ok(Some(tuple))
    }
}
