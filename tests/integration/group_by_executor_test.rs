use std::collections::HashMap;
use vaultgres::catalog::{TableSchema, Value};
use vaultgres::executor::operators::executor::Executor;
use vaultgres::executor::GroupByExecutor;
use vaultgres::parser::ast::{AggregateFunc, Expr};

type Tuple = HashMap<String, Value>;

struct MockExecutorForGroupBy {
    tuples: Vec<Tuple>,
    idx: usize,
}

impl MockExecutorForGroupBy {
    fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples, idx: 0 }
    }
}

impl Executor for MockExecutorForGroupBy {
    fn next(
        &mut self,
    ) -> Result<Option<Tuple>, vaultgres::executor::operators::executor::ExecutorError> {
        if self.idx >= self.tuples.len() {
            Ok(None)
        } else {
            self.idx += 1;
            Ok(Some(self.tuples[self.idx - 1].clone()))
        }
    }
}

fn make_tuple(pairs: Vec<(&str, Value)>) -> Tuple {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

#[test]
fn test_group_by_executor_counts() {
    let tuples = vec![
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(10))]),
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(20))]),
        make_tuple(vec![("category", Value::Text("B".to_string())), ("value", Value::Int(30))]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("category".to_string())];
    let aggregates =
        vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

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
                assert_eq!(r.get("count(*)"), Some(&Value::Int(2)));
                found_a = true;
            }
            Some(Value::Text(s)) if s == "B" => {
                assert_eq!(r.get("count(*)"), Some(&Value::Int(1)));
                found_b = true;
            }
            _ => {}
        }
    }
    assert!(found_a && found_b);
}

#[test]
fn test_group_by_executor_sums() {
    let tuples = vec![
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(10))]),
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(20))]),
        make_tuple(vec![("category", Value::Text("B".to_string())), ("value", Value::Int(30))]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("category".to_string())];
    let aggregates = vec![Expr::Aggregate {
        func: AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    }];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let mut results: Vec<Tuple> = Vec::new();
    while let Some(tuple) = executor.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    for r in results {
        match r.get("category") {
            Some(Value::Text(s)) if s == "A" => {
                assert_eq!(r.get("sum(value)"), Some(&Value::Int(30)));
            }
            Some(Value::Text(s)) if s == "B" => {
                assert_eq!(r.get("sum(value)"), Some(&Value::Int(30)));
            }
            _ => {}
        }
    }
}

#[test]
fn test_group_by_executor_multiple_aggregates() {
    let tuples = vec![
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(10))]),
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(20))]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("category".to_string())];
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
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let result = executor.next().unwrap().unwrap();
    assert_eq!(result.get("count(*)"), Some(&Value::Int(2)));
    assert_eq!(result.get("sum(value)"), Some(&Value::Int(30)));
    assert_eq!(result.get("avg(value)"), Some(&Value::Int(15)));
    assert_eq!(result.get("min(value)"), Some(&Value::Int(10)));
    assert_eq!(result.get("max(value)"), Some(&Value::Int(20)));
    assert!(executor.next().unwrap().is_none());
}

#[test]
fn test_group_by_executor_empty_input() {
    let tuples: Vec<Tuple> = vec![];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("category".to_string())];
    let aggregates =
        vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let result = executor.next().unwrap();
    assert!(result.is_none());
}

#[test]
fn test_group_by_executor_with_nulls() {
    let tuples = vec![
        make_tuple(vec![("category", Value::Null), ("value", Value::Int(10))]),
        make_tuple(vec![("category", Value::Null), ("value", Value::Int(20))]),
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(30))]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("category".to_string())];
    let aggregates = vec![Expr::Aggregate {
        func: AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    }];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let mut results: Vec<Tuple> = Vec::new();
    while let Some(tuple) = executor.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
}

#[test]
fn test_group_by_executor_single_group() {
    let tuples = vec![
        make_tuple(vec![("value", Value::Int(10))]),
        make_tuple(vec![("value", Value::Int(20))]),
        make_tuple(vec![("value", Value::Int(30))]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![];
    let aggregates = vec![Expr::Aggregate {
        func: AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    }];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let result = executor.next().unwrap().unwrap();
    assert_eq!(result.get("sum(value)"), Some(&Value::Int(60)));
    assert!(executor.next().unwrap().is_none());
}

#[test]
fn test_group_by_executor_multiple_columns() {
    let tuples = vec![
        make_tuple(vec![
            ("a", Value::Text("x".to_string())),
            ("b", Value::Text("y".to_string())),
            ("c", Value::Int(10)),
        ]),
        make_tuple(vec![
            ("a", Value::Text("x".to_string())),
            ("b", Value::Text("y".to_string())),
            ("c", Value::Int(20)),
        ]),
        make_tuple(vec![
            ("a", Value::Text("x".to_string())),
            ("b", Value::Text("z".to_string())),
            ("c", Value::Int(30)),
        ]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("a".to_string()), Expr::Column("b".to_string())];
    let aggregates = vec![Expr::Aggregate {
        func: AggregateFunc::Sum,
        arg: Box::new(Expr::Column("c".to_string())),
    }];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let mut results: Vec<Tuple> = Vec::new();
    while let Some(tuple) = executor.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
}

#[test]
fn test_group_by_executor_min_max() {
    let tuples = vec![
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(10))]),
        make_tuple(vec![("category", Value::Text("A".to_string())), ("value", Value::Int(30))]),
        make_tuple(vec![("category", Value::Text("B".to_string())), ("value", Value::Int(20))]),
    ];
    let child = Box::new(MockExecutorForGroupBy::new(tuples));
    let group_by = vec![Expr::Column("category".to_string())];
    let aggregates = vec![
        Expr::Aggregate {
            func: AggregateFunc::Min,
            arg: Box::new(Expr::Column("value".to_string())),
        },
        Expr::Aggregate {
            func: AggregateFunc::Max,
            arg: Box::new(Expr::Column("value".to_string())),
        },
    ];
    let output_schema = TableSchema::new("group_by".to_string(), vec![]);
    let mut executor = GroupByExecutor::new(child, group_by, aggregates, output_schema).unwrap();

    let mut results: Vec<Tuple> = Vec::new();
    while let Some(tuple) = executor.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    for r in results {
        match r.get("category") {
            Some(Value::Text(s)) if s == "A" => {
                assert_eq!(r.get("min(value)"), Some(&Value::Int(10)));
                assert_eq!(r.get("max(value)"), Some(&Value::Int(30)));
            }
            Some(Value::Text(s)) if s == "B" => {
                assert_eq!(r.get("min(value)"), Some(&Value::Int(20)));
                assert_eq!(r.get("max(value)"), Some(&Value::Int(20)));
            }
            _ => {}
        }
    }
}
