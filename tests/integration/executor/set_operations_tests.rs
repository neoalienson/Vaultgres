use vaultgres::executor::{Except, Intersect, MockExecutor, SimpleExecutor, SimpleTuple, Union};

#[test]
fn test_union() {
    let left =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }]);
    let right =
        MockExecutor::new(vec![SimpleTuple { data: vec![2] }, SimpleTuple { data: vec![3] }]);

    let mut union = Union::new(Box::new(left), Box::new(right), false);
    union.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = union.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 3);
    union.close().unwrap();
}

#[test]
fn test_union_all() {
    let left =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }]);
    let right =
        MockExecutor::new(vec![SimpleTuple { data: vec![2] }, SimpleTuple { data: vec![3] }]);

    let mut union = Union::new(Box::new(left), Box::new(right), true);
    union.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = union.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 4);
    union.close().unwrap();
}

#[test]
fn test_intersect() {
    let left = MockExecutor::new(vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![3] },
    ]);
    let right = MockExecutor::new(vec![
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![3] },
        SimpleTuple { data: vec![4] },
    ]);

    let mut intersect = Intersect::new(Box::new(left), Box::new(right));
    intersect.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = intersect.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 2);
    intersect.close().unwrap();
}

#[test]
fn test_except() {
    let left = MockExecutor::new(vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![3] },
    ]);
    let right = MockExecutor::new(vec![
        SimpleTuple { data: vec![2] },
        SimpleTuple { data: vec![3] },
        SimpleTuple { data: vec![4] },
    ]);

    let mut except = Except::new(Box::new(left), Box::new(right));
    except.open().unwrap();

    let mut results = Vec::new();
    while let Some(tuple) = except.next().unwrap() {
        results.push(tuple);
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data[0], 1);
    except.close().unwrap();
}
