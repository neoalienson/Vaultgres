use vaultgres::executor::{Distinct, GroupBy, Having, MockExecutor, SimpleExecutor, SimpleTuple};

#[test]
fn test_group_by() {
    let input = MockExecutor::new(vec![
        SimpleTuple { data: vec![1, 10] },
        SimpleTuple { data: vec![1, 20] },
        SimpleTuple { data: vec![2, 30] },
        SimpleTuple { data: vec![2, 40] },
    ]);

    let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
    group_by.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = group_by.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    group_by.close().unwrap();
}

#[test]
fn test_having() {
    let input = MockExecutor::new(vec![
        SimpleTuple { data: vec![1, 30] },
        SimpleTuple { data: vec![2, 10] },
        SimpleTuple { data: vec![3, 50] },
    ]);

    let mut having =
        Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 20));
    having.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = having.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    having.close().unwrap();
}

#[test]
fn test_distinct() {
    let input = MockExecutor::new(vec![
        SimpleTuple { data: vec![1, 2] },
        SimpleTuple { data: vec![1, 2] },
        SimpleTuple { data: vec![3, 4] },
        SimpleTuple { data: vec![1, 2] },
    ]);

    let mut distinct = Distinct::new(Box::new(input));
    distinct.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = distinct.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    distinct.close().unwrap();
}
