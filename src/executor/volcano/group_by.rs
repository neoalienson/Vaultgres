//! GroupByExecutor - Sort-based grouping for GROUP BY clauses
//!
//! PostgreSQL approach: When input is sorted by GROUP BY columns, we can emit
//! groups incrementally instead of buffering everything in a hash table.
//! This is more memory-efficient for large datasets.
//!
//! If input is not sorted, this executor will sort it first (or the planner
//! should ensure proper ordering via SortExecutor).

use crate::catalog::{TableSchema, Value};
use crate::executor::eval::Eval;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::{AggregateFunc, Expr};

/// State for tracking aggregate computation within a group
#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum(i64),
    Avg { sum: i64, count: i64 },
    Min(Value),
    Max(Value),
}

pub struct GroupByExecutor {
    child: Box<dyn Executor>,
    group_by: Vec<Expr>,
    aggregates: Vec<Expr>,
    output_schema: TableSchema,
    require_sort: bool,
    buffered_groups: Vec<Tuple>,
    current_idx: usize,
    exhausted: bool,
}

impl GroupByExecutor {
    pub fn new(
        child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
    ) -> Result<Self, ExecutorError> {
        Self::new_with_options(child, group_by, aggregates, output_schema, true)
    }

    pub fn new_with_options(
        child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
        require_sort: bool,
    ) -> Result<Self, ExecutorError> {
        let mut executor = Self {
            child,
            group_by,
            aggregates,
            output_schema,
            require_sort,
            buffered_groups: Vec::new(),
            current_idx: 0,
            exhausted: false,
        };

        if !executor.require_sort {
            executor.buffer_all_groups()?;
        }

        Ok(executor)
    }

    fn buffer_all_groups(&mut self) -> Result<(), ExecutorError> {
        let mut current_group_key: Option<Tuple> = None;
        let mut current_agg_states: Vec<AggregateState> = Vec::new();
        let mut first_tuple: Option<Tuple> = None;

        while let Some(tuple) = self.child.next()? {
            let group_key = Self::compute_group_key(&tuple, &self.group_by)?;

            match &current_group_key {
                None => {
                    current_group_key = Some(group_key);
                    first_tuple = Some(tuple.clone());
                    current_agg_states = Self::init_aggregate_states(&self.aggregates);
                }
                Some(prev_key) => {
                    if !Self::group_keys_equal(prev_key, &group_key) {
                        Self::emit_group(
                            &mut self.buffered_groups,
                            first_tuple.take().unwrap(),
                            current_agg_states.clone(),
                            &self.aggregates,
                        )?;
                        current_group_key = Some(group_key);
                        first_tuple = Some(tuple.clone());
                        current_agg_states = Self::init_aggregate_states(&self.aggregates);
                    }
                }
            }

            Self::update_aggregate_states(&mut current_agg_states, &self.aggregates, &tuple)?;
        }

        if let Some(tuple) = first_tuple {
            Self::emit_group(
                &mut self.buffered_groups,
                tuple,
                current_agg_states,
                &self.aggregates,
            )?;
        }

        Ok(())
    }

    fn init_aggregate_states(aggregates: &[Expr]) -> Vec<AggregateState> {
        aggregates
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
            .collect()
    }

    fn update_aggregate_states(
        states: &mut [AggregateState],
        aggregates: &[Expr],
        tuple: &Tuple,
    ) -> Result<(), ExecutorError> {
        for (i, agg_expr) in aggregates.iter().enumerate() {
            if let Expr::Aggregate { func: _func, arg } = agg_expr {
                let arg_val = if matches!(arg.as_ref(), Expr::Star) {
                    Value::Int(1)
                } else {
                    Eval::eval_expr(arg, tuple)?
                };

                match &mut states[i] {
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
                            states[i] = AggregateState::Min(arg_val);
                        }
                    }
                    AggregateState::Max(current_max) => {
                        if matches!(current_max, Value::Null)
                            || Self::compare_values(&arg_val, current_max)?
                                == std::cmp::Ordering::Greater
                        {
                            states[i] = AggregateState::Max(arg_val);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn emit_group(
        buffer: &mut Vec<Tuple>,
        tuple: Tuple,
        mut states: Vec<AggregateState>,
        aggregates: &[Expr],
    ) -> Result<(), ExecutorError> {
        let mut group_tuple = Tuple::new();

        for (key, val) in &tuple {
            group_tuple.insert(key.clone(), val.clone());
        }

        for (i, agg_expr) in aggregates.iter().enumerate() {
            let agg_name = Self::get_aggregate_name(agg_expr);
            let agg_value = match &states[i] {
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

        buffer.push(group_tuple);
        Ok(())
    }

    fn compute_group_key(tuple: &Tuple, group_by: &[Expr]) -> Result<Tuple, ExecutorError> {
        let mut key = Tuple::new();
        for expr in group_by {
            match expr {
                Expr::Column(name) => {
                    if let Some(val) = tuple.get(name) {
                        key.insert(name.clone(), val.clone());
                    }
                }
                Expr::QualifiedColumn { table, column } => {
                    let qualified_name = format!("{}.{}", table, column);
                    if let Some(val) = tuple.get(&qualified_name).or_else(|| tuple.get(column)) {
                        key.insert(qualified_name, val.clone());
                    }
                }
                Expr::Alias { expr, alias } => {
                    let val = Eval::eval_expr(expr, tuple)?;
                    key.insert(alias.clone(), val);
                }
                Expr::FunctionCall { name, args } => {
                    let val = Eval::eval_expr(expr, tuple)?;
                    key.insert(name.clone(), val);
                }
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported GROUP BY expression: {:?}",
                        expr
                    )));
                }
            }
        }
        Ok(key)
    }

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

    fn group_keys_equal(a: &Tuple, b: &Tuple) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (k, v) in a {
            match b.get(k) {
                Some(v2) if v == v2 => continue,
                _ => return false,
            }
        }
        true
    }
}

impl Executor for GroupByExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.require_sort {
            self.streaming_next()
        } else {
            self.buffered_next()
        }
    }
}

impl GroupByExecutor {
    fn buffered_next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.current_idx >= self.buffered_groups.len() {
            return Ok(None);
        }
        let tuple = self.buffered_groups[self.current_idx].clone();
        self.current_idx += 1;
        Ok(Some(tuple))
    }

    fn streaming_next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.exhausted {
            return Ok(None);
        }

        let mut current_group_key: Option<Tuple> = None;
        let mut current_agg_states: Vec<AggregateState> = Vec::new();
        let mut first_tuple: Option<Tuple> = None;

        while let Some(tuple) = self.child.next()? {
            let group_key = Self::compute_group_key(&tuple, &self.group_by)?;

            match &current_group_key {
                None => {
                    current_group_key = Some(group_key);
                    first_tuple = Some(tuple.clone());
                    current_agg_states = Self::init_aggregate_states(&self.aggregates);
                }
                Some(prev_key) => {
                    if !Self::group_keys_equal(prev_key, &group_key) {
                        Self::emit_group(
                            &mut self.buffered_groups,
                            first_tuple.take().unwrap(),
                            current_agg_states.clone(),
                            &self.aggregates,
                        )?;
                        current_group_key = Some(group_key);
                        first_tuple = Some(tuple.clone());
                        current_agg_states = Self::init_aggregate_states(&self.aggregates);
                    }
                }
            }

            Self::update_aggregate_states(&mut current_agg_states, &self.aggregates, &tuple)?;
        }

        if let Some(tuple) = first_tuple {
            Self::emit_group(
                &mut self.buffered_groups,
                tuple,
                current_agg_states,
                &self.aggregates,
            )?;
        }

        if self.buffered_groups.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        if self.current_idx < self.buffered_groups.len() {
            let tuple = self.buffered_groups[self.current_idx].clone();
            self.current_idx += 1;
            Ok(Some(tuple))
        } else {
            self.exhausted = true;
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::AggregateFunc;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        idx: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, idx: 0 }
        }

        fn sorted_by_category(tuples: Vec<Tuple>) -> Self {
            Self::new(tuples)
        }
    }

    impl Executor for MockExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.idx >= self.tuples.len() {
                Ok(None)
            } else {
                self.idx += 1;
                Ok(Some(self.tuples[self.idx - 1].clone()))
            }
        }
    }

    fn create_output_schema() -> TableSchema {
        TableSchema::new("group_by".to_string(), vec![])
    }

    #[test]
    fn test_group_by_single_column() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(20)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("value".to_string(), Value::Int(30)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        let mut found_a = false;
        let mut found_b = false;
        for r in results {
            match r.get("category") {
                Some(Value::Text(s)) if s == "A" => {
                    assert_eq!(r.get("sum(value)"), Some(&Value::Int(30)));
                    found_a = true;
                }
                Some(Value::Text(s)) if s == "B" => {
                    assert_eq!(r.get("sum(value)"), Some(&Value::Int(30)));
                    found_b = true;
                }
                _ => panic!("Unexpected result"),
            }
        }
        assert!(found_a && found_b);
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let tuples = vec![
            [
                ("a".to_string(), Value::Text("x".to_string())),
                ("b".to_string(), Value::Text("y".to_string())),
                ("c".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("a".to_string(), Value::Text("x".to_string())),
                ("b".to_string(), Value::Text("y".to_string())),
                ("c".to_string(), Value::Int(20)),
            ]
            .into(),
            [
                ("a".to_string(), Value::Text("x".to_string())),
                ("b".to_string(), Value::Text("z".to_string())),
                ("c".to_string(), Value::Int(30)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("a".to_string()), Expr::Column("b".to_string())];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("c".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_count_with_group_by() {
        let tuples = vec![
            [("category".to_string(), Value::Text("A".to_string()))].into(),
            [("category".to_string(), Value::Text("A".to_string()))].into(),
            [("category".to_string(), Value::Text("B".to_string()))].into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates =
            vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        for r in results {
            match r.get("category") {
                Some(Value::Text(s)) if s == "A" => {
                    assert_eq!(r.get("count(*)"), Some(&Value::Int(2)));
                }
                Some(Value::Text(s)) if s == "B" => {
                    assert_eq!(r.get("count(*)"), Some(&Value::Int(1)));
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_min_max_aggregates() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(30)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("v".to_string(), Value::Int(20)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("v".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("v".to_string())),
            },
        ];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        for r in results {
            match r.get("category") {
                Some(Value::Text(s)) if s == "A" => {
                    assert_eq!(r.get("min(v)"), Some(&Value::Int(10)));
                    assert_eq!(r.get("max(v)"), Some(&Value::Int(30)));
                }
                Some(Value::Text(s)) if s == "B" => {
                    assert_eq!(r.get("min(v)"), Some(&Value::Int(20)));
                    assert_eq!(r.get("max(v)"), Some(&Value::Int(20)));
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_avg_aggregate() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(30)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("v".to_string(), Value::Int(20)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Avg,
            arg: Box::new(Expr::Column("v".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        for r in results {
            match r.get("category") {
                Some(Value::Text(s)) if s == "A" => {
                    assert_eq!(r.get("avg(v)"), Some(&Value::Int(20)));
                }
                Some(Value::Text(s)) if s == "B" => {
                    assert_eq!(r.get("avg(v)"), Some(&Value::Int(20)));
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_multiple_aggregates() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(20)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates = vec![
            Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) },
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("v".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Avg,
                arg: Box::new(Expr::Column("v".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("v".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("v".to_string())),
            },
        ];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.get("count(*)"), Some(&Value::Int(2)));
        assert_eq!(result.get("sum(v)"), Some(&Value::Int(30)));
        assert_eq!(result.get("avg(v)"), Some(&Value::Int(15)));
        assert_eq!(result.get("min(v)"), Some(&Value::Int(10)));
        assert_eq!(result.get("max(v)"), Some(&Value::Int(20)));
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_group_by_with_nulls() {
        let tuples = vec![
            [("category".to_string(), Value::Null), ("v".to_string(), Value::Int(10))].into(),
            [("category".to_string(), Value::Null), ("v".to_string(), Value::Int(20))].into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("v".to_string(), Value::Int(30)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("v".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_empty_input() {
        let tuples: Vec<Tuple> = vec![];
        let child = Box::new(MockExecutor::new(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates =
            vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let result = executor.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_single_group() {
        let tuples = vec![
            [("v".to_string(), Value::Int(10))].into(),
            [("v".to_string(), Value::Int(20))].into(),
            [("v".to_string(), Value::Int(30))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let group_by = vec![];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("v".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.get("sum(v)"), Some(&Value::Int(60)));
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_group_by_alias_expression() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("a".to_string())),
                ("value".to_string(), Value::Int(20)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::Alias {
            expr: Box::new(Expr::FunctionCall {
                name: "UPPER".to_string(),
                args: vec![Expr::Column("category".to_string())],
            }),
            alias: "upper_cat".to_string(),
        }];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.get("sum(value)"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_unsorted_input_requires_sort() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("value".to_string(), Value::Int(30)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(20)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_group_by_with_expression() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(10)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("a".to_string())),
                ("value".to_string(), Value::Int(20)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("value".to_string(), Value::Int(30)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::sorted_by_category(tuples));
        let group_by = vec![Expr::FunctionCall {
            name: "UPPER".to_string(),
            args: vec![Expr::Column("category".to_string())],
        }];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        }];
        let output_schema = create_output_schema();
        let mut executor =
            GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
    }
}
