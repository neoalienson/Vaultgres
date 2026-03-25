use std::sync::Arc;
use vaultgres::catalog::{Catalog, TableSchema, Value};
use vaultgres::executor::operators::executor::Tuple;
use vaultgres::executor::parallel::morsel::Morsel;
use vaultgres::executor::parallel::operator::ParallelOperator;
use vaultgres::executor::volcano::window::{create_window_info, WindowExecutor};
use vaultgres::parser::ast::{AggregateFunc, OrderByExpr, WindowFunc};

struct MockExecutor {
    tuples: Vec<Tuple>,
}

impl MockExecutor {
    fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples }
    }
}

impl vaultgres::executor::operators::executor::Executor for MockExecutor {
    fn next(
        &mut self,
    ) -> Result<Option<Tuple>, vaultgres::executor::operators::executor::ExecutorError> {
        if self.tuples.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.tuples.remove(0)))
        }
    }
}

struct MockParallelOperator {
    tuples: Vec<Tuple>,
}

impl ParallelOperator for MockParallelOperator {
    fn process_morsel(
        &self,
        mut morsel: Morsel,
    ) -> Result<Morsel, vaultgres::executor::operators::executor::ExecutorError> {
        morsel.tuples = self.tuples.clone();
        Ok(morsel)
    }
}

#[test]
fn test_window_row_number_integration() {
    let tuples: Vec<Tuple> = (0..10)
        .map(|i| {
            let mut tuple = std::collections::HashMap::new();
            tuple.insert("id".to_string(), Value::Int(i));
            tuple.insert("category".to_string(), Value::Int(i % 3));
            tuple
        })
        .collect();

    let windows = vec![create_window_info(
        WindowFunc::RowNumber,
        Box::new(vaultgres::parser::ast::Expr::Star),
        vec![],
        vec![],
        None,
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let mut count = 0;
    while let Some(tuple) = window_executor.next().unwrap() {
        let rn = tuple.get("row_number").unwrap();
        assert!(matches!(rn, Value::Int(n) if *n > 0));
        count += 1;
    }
    assert_eq!(count, 10);
}

#[test]
fn test_window_rank_with_duplicates_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("salary".to_string(), Value::Int(5000));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("salary".to_string(), Value::Int(6000));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("salary".to_string(), Value::Int(6000));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("salary".to_string(), Value::Int(7000));
            t
        },
    ];

    let mut windows = vec![create_window_info(
        WindowFunc::Rank,
        Box::new(vaultgres::parser::ast::Expr::Column("salary".to_string())),
        vec!["dept".to_string()],
        vec![OrderByExpr { column: "salary".to_string(), ascending: true }],
        None,
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("rank"), Some(&Value::Int(1)));
    assert_eq!(results[1].get("rank"), Some(&Value::Int(2)));
    assert_eq!(results[2].get("rank"), Some(&Value::Int(2)));
    assert_eq!(results[3].get("rank"), Some(&Value::Int(4)));
}

#[test]
fn test_window_multiple_functions_integration() {
    let tuples: Vec<Tuple> = (0..5)
        .map(|i| {
            let mut tuple = std::collections::HashMap::new();
            tuple.insert("id".to_string(), Value::Int(i));
            tuple.insert("value".to_string(), Value::Int((i + 1) * 10));
            tuple
        })
        .collect();

    let windows = vec![
        create_window_info(
            WindowFunc::RowNumber,
            Box::new(vaultgres::parser::ast::Expr::Star),
            vec![],
            vec![],
            None,
        ),
        create_window_info(
            WindowFunc::Lag,
            Box::new(vaultgres::parser::ast::Expr::Column("value".to_string())),
            vec![],
            vec![],
            None,
        ),
        create_window_info(
            WindowFunc::Lead,
            Box::new(vaultgres::parser::ast::Expr::Column("value".to_string())),
            vec![],
            vec![],
            None,
        ),
    ];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("row_number"), Some(&Value::Int(1)));
    assert_eq!(results[0].get("lag"), Some(&Value::Null));
    assert_eq!(results[0].get("lead"), Some(&Value::Int(20)));
}

#[test]
fn test_window_partition_aware_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(200));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(50));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(150));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("dept".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(300));
            t
        },
    ];

    let windows = vec![create_window_info(
        WindowFunc::RowNumber,
        Box::new(vaultgres::parser::ast::Expr::Star),
        vec!["dept".to_string()],
        vec![],
        None,
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("row_number"), Some(&Value::Int(1)));
    assert_eq!(results[1].get("row_number"), Some(&Value::Int(2)));
    assert_eq!(results[2].get("row_number"), Some(&Value::Int(1)));
    assert_eq!(results[3].get("row_number"), Some(&Value::Int(2)));
    assert_eq!(results[4].get("row_number"), Some(&Value::Int(3)));
}

fn create_window_info_with_frame(
    func: WindowFunc,
    arg: Box<vaultgres::parser::ast::Expr>,
    partition_by: Vec<String>,
    order_by: Vec<OrderByExpr>,
    frame: vaultgres::parser::ast::WindowFrame,
) -> vaultgres::executor::volcano::window::WindowInfo {
    create_window_info(func, arg, partition_by, order_by, Some(frame))
}

#[test]
fn test_window_rows_frame_with_offset_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(200));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(3));
            t.insert("value".to_string(), Value::Int(300));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(4));
            t.insert("value".to_string(), Value::Int(400));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(5));
            t.insert("value".to_string(), Value::Int(500));
            t
        },
    ];

    let windows = vec![create_window_info_with_frame(
        WindowFunc::FirstValue,
        Box::new(vaultgres::parser::ast::Expr::Column("value".to_string())),
        vec![],
        vec![OrderByExpr { column: "id".to_string(), ascending: true }],
        vaultgres::parser::ast::WindowFrame {
            mode: vaultgres::parser::ast::WindowFrameMode::Rows,
            start: vaultgres::parser::ast::WindowFrameBound::Preceding(2),
            end: Some(vaultgres::parser::ast::WindowFrameBound::Following(2)),
        },
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("first_value"), Some(&Value::Int(100)));
    assert_eq!(results[1].get("first_value"), Some(&Value::Int(100)));
    assert_eq!(results[2].get("first_value"), Some(&Value::Int(100)));
    assert_eq!(results[3].get("first_value"), Some(&Value::Int(200)));
    assert_eq!(results[4].get("first_value"), Some(&Value::Int(300)));
}

#[test]
fn test_window_range_frame_with_peers_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(3));
            t.insert("value".to_string(), Value::Int(200));
            t
        },
    ];

    let windows = vec![create_window_info_with_frame(
        WindowFunc::FirstValue,
        Box::new(vaultgres::parser::ast::Expr::Column("value".to_string())),
        vec![],
        vec![OrderByExpr { column: "value".to_string(), ascending: true }],
        vaultgres::parser::ast::WindowFrame {
            mode: vaultgres::parser::ast::WindowFrameMode::Range,
            start: vaultgres::parser::ast::WindowFrameBound::UnboundedPreceding,
            end: Some(vaultgres::parser::ast::WindowFrameBound::CurrentRow),
        },
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("first_value"), Some(&Value::Int(100)));
    assert_eq!(results[1].get("first_value"), Some(&Value::Int(100)));
    assert_eq!(results[2].get("first_value"), Some(&Value::Int(100)));
}

#[test]
fn test_window_lag_with_offset_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(200));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(3));
            t.insert("value".to_string(), Value::Int(300));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(4));
            t.insert("value".to_string(), Value::Int(400));
            t
        },
    ];

    let windows = vec![create_window_info_with_frame(
        WindowFunc::Lag,
        Box::new(vaultgres::parser::ast::Expr::Tuple(vec![
            vaultgres::parser::ast::Expr::Column("value".to_string()),
            vaultgres::parser::ast::Expr::Number(2),
        ])),
        vec![],
        vec![OrderByExpr { column: "id".to_string(), ascending: true }],
        vaultgres::parser::ast::WindowFrame {
            mode: vaultgres::parser::ast::WindowFrameMode::Rows,
            start: vaultgres::parser::ast::WindowFrameBound::UnboundedPreceding,
            end: Some(vaultgres::parser::ast::WindowFrameBound::UnboundedFollowing),
        },
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("lag"), Some(&Value::Null));
    assert_eq!(results[1].get("lag"), Some(&Value::Null));
    assert_eq!(results[2].get("lag"), Some(&Value::Int(100)));
    assert_eq!(results[3].get("lag"), Some(&Value::Int(200)));
}

#[test]
fn test_window_ntile_with_buckets_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(200));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(3));
            t.insert("value".to_string(), Value::Int(300));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(4));
            t.insert("value".to_string(), Value::Int(400));
            t
        },
    ];

    let windows = vec![create_window_info_with_frame(
        WindowFunc::Ntile,
        Box::new(vaultgres::parser::ast::Expr::Number(2)),
        vec![],
        vec![OrderByExpr { column: "id".to_string(), ascending: true }],
        vaultgres::parser::ast::WindowFrame {
            mode: vaultgres::parser::ast::WindowFrameMode::Rows,
            start: vaultgres::parser::ast::WindowFrameBound::UnboundedPreceding,
            end: Some(vaultgres::parser::ast::WindowFrameBound::UnboundedFollowing),
        },
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("ntile"), Some(&Value::Int(1)));
    assert_eq!(results[1].get("ntile"), Some(&Value::Int(1)));
    assert_eq!(results[2].get("ntile"), Some(&Value::Int(2)));
    assert_eq!(results[3].get("ntile"), Some(&Value::Int(2)));
}

#[test]
fn test_window_nth_value_within_frame_integration() {
    let tuples: Vec<Tuple> = vec![
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(1));
            t.insert("value".to_string(), Value::Int(100));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(2));
            t.insert("value".to_string(), Value::Int(200));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(3));
            t.insert("value".to_string(), Value::Int(300));
            t
        },
        {
            let mut t = std::collections::HashMap::new();
            t.insert("id".to_string(), Value::Int(4));
            t.insert("value".to_string(), Value::Int(400));
            t
        },
    ];

    let windows = vec![create_window_info_with_frame(
        WindowFunc::NthValue,
        Box::new(vaultgres::parser::ast::Expr::Tuple(vec![
            vaultgres::parser::ast::Expr::Column("value".to_string()),
            vaultgres::parser::ast::Expr::Number(2),
        ])),
        vec![],
        vec![OrderByExpr { column: "id".to_string(), ascending: true }],
        vaultgres::parser::ast::WindowFrame {
            mode: vaultgres::parser::ast::WindowFrameMode::Rows,
            start: vaultgres::parser::ast::WindowFrameBound::UnboundedPreceding,
            end: Some(vaultgres::parser::ast::WindowFrameBound::UnboundedFollowing),
        },
    )];
    let schema = TableSchema::new("t".to_string(), vec![]);

    let executor = MockExecutor::new(tuples);
    let mut window_executor = WindowExecutor::new(Box::new(executor), windows, schema).unwrap();

    let results: Vec<Tuple> = std::iter::from_fn(|| window_executor.next().unwrap()).collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("nth_value"), Some(&Value::Int(200)));
    assert_eq!(results[1].get("nth_value"), Some(&Value::Int(300)));
    assert_eq!(results[2].get("nth_value"), Some(&Value::Int(400)));
    assert_eq!(results[3].get("nth_value"), Some(&Value::Null));
}
