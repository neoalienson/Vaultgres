// Integration tests for executor operators
use vaultgres::executor::{
    Except, Intersect, Join, JoinType, MockExecutor, SimpleExecutor, SimpleTuple, Union,
};

#[test]
fn test_join_execution() {
    let left = MockExecutor::new(vec![SimpleTuple { data: vec![1] }]);
    let right = MockExecutor::new(vec![SimpleTuple { data: vec![1] }]);
    let mut join = Join::new(
        Box::new(left),
        Box::new(right),
        JoinType::Inner,
        Box::new(|l, r| l.data[0] == r.data[0]),
    );
    join.open().unwrap();
    assert!(join.next().unwrap().is_some());
    join.close().unwrap();
}

#[test]
fn test_union_execution() {
    let left = MockExecutor::new(vec![SimpleTuple { data: vec![1] }]);
    let right = MockExecutor::new(vec![SimpleTuple { data: vec![2] }]);
    let mut union = Union::new(Box::new(left), Box::new(right), false);
    union.open().unwrap();
    let mut count = 0;
    while union.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2);
    union.close().unwrap();
}

#[test]
fn test_intersect_execution() {
    let left =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }]);
    let right = MockExecutor::new(vec![SimpleTuple { data: vec![2] }]);
    let mut intersect = Intersect::new(Box::new(left), Box::new(right));
    intersect.open().unwrap();
    let result = intersect.next().unwrap().unwrap();
    assert_eq!(result.data[0], 2);
    intersect.close().unwrap();
}

#[test]
fn test_except_execution() {
    let left =
        MockExecutor::new(vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }]);
    let right = MockExecutor::new(vec![SimpleTuple { data: vec![2] }]);
    let mut except = Except::new(Box::new(left), Box::new(right));
    except.open().unwrap();
    let result = except.next().unwrap().unwrap();
    assert_eq!(result.data[0], 1);
    except.close().unwrap();
}
