use vaultgres::executor::{Join, JoinType, MockExecutor, SimpleExecutor, SimpleTuple};

#[test]
fn test_inner_join() {
    let left = MockExecutor::new(vec![
        SimpleTuple { data: vec![1, 10] },
        SimpleTuple { data: vec![2, 20] },
    ]);
    let right = MockExecutor::new(vec![
        SimpleTuple { data: vec![1, 100] },
        SimpleTuple { data: vec![2, 200] },
    ]);

    let mut join = Join::new(
        Box::new(left),
        Box::new(right),
        JoinType::Inner,
        Box::new(|l, r| l.data[0] == r.data[0]),
    );
    join.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = join.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    join.close().unwrap();
}

#[test]
fn test_left_join() {
    let left = MockExecutor::new(vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![3] },
    ]);
    let right =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }]);

    let mut join = Join::new(
        Box::new(left),
        Box::new(right),
        JoinType::Left,
        Box::new(|l, r| l.data[0] == r.data[0]),
    );
    join.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = join.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 3);
    join.close().unwrap();
}

#[test]
fn test_right_join() {
    let left =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }]);
    let right = MockExecutor::new(vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![3] },
    ]);

    let mut join = Join::new(
        Box::new(left),
        Box::new(right),
        JoinType::Right,
        Box::new(|l, r| l.data[0] == r.data[0]),
    );
    join.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = join.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 3);
    join.close().unwrap();
}

#[test]
fn test_full_join() {
    let left = MockExecutor::new(vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![4] },
    ]);
    let right =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![3] }]);

    let mut join = Join::new(
        Box::new(left),
        Box::new(right),
        JoinType::Full,
        Box::new(|l, r| l.data[0] == r.data[0]),
    );
    join.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = join.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 4);
    join.close().unwrap();
}
