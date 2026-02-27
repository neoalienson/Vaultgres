use rustgres::storage::{BufferPool, PageId};
use rustgres::storage::heap::HeapFile;
use rustgres::executor::{Executor, SeqScan, Filter, Project, NestedLoopJoin, Tuple};
use rustgres::parser::Expr;
use std::sync::Arc;
use std::collections::HashMap;

#[test]
fn test_seq_scan_integration() {
    let pool = Arc::new(BufferPool::new(10));
    let heap = Arc::new(HeapFile::new(pool));
    
    heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();
    heap.insert_tuple(PageId(0), vec![4, 5, 6]).unwrap();
    heap.insert_tuple(PageId(0), vec![7, 8, 9]).unwrap();
    
    let mut scan = SeqScan::new(heap, "data".to_string());
    scan.open().unwrap();
    
    let mut count = 0;
    while let Some(_) = scan.next().unwrap() {
        count += 1;
    }
    
    assert_eq!(count, 3);
    scan.close().unwrap();
}

#[test]
fn test_filter_integration() {
    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new() -> Self {
            let mut t1 = HashMap::new();
            t1.insert("id".to_string(), b"15".to_vec());
            let mut t2 = HashMap::new();
            t2.insert("id".to_string(), b"20".to_vec());
            let mut t3 = HashMap::new();
            t3.insert("id".to_string(), b"15".to_vec());
            
            Self { tuples: vec![t1, t2, t3], index: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, rustgres::executor::ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            Ok(())
        }
    }

    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: rustgres::parser::BinaryOperator::Equals,
        right: Box::new(Expr::Number(15)),
    };

    let mut filter = Filter::new(Box::new(MockExecutor::new()), predicate);
    filter.open().unwrap();

    let t1 = filter.next().unwrap();
    assert!(t1.is_some());
    assert_eq!(t1.unwrap().get("id").unwrap(), b"15");

    let t2 = filter.next().unwrap();
    assert!(t2.is_some());
    assert_eq!(t2.unwrap().get("id").unwrap(), b"15");

    assert!(filter.next().unwrap().is_none());
}

#[test]
fn test_project_integration() {
    struct MockExecutor {
        tuple: Option<Tuple>,
    }

    impl MockExecutor {
        fn new() -> Self {
            let mut t = HashMap::new();
            t.insert("id".to_string(), b"1".to_vec());
            t.insert("name".to_string(), b"Alice".to_vec());
            t.insert("age".to_string(), b"30".to_vec());
            
            Self { tuple: Some(t) }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, rustgres::executor::ExecutorError> {
            Ok(self.tuple.take())
        }

        fn close(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            Ok(())
        }
    }

    let mut project = Project::new(
        Box::new(MockExecutor::new()),
        vec!["id".to_string(), "name".to_string()]
    );
    
    project.open().unwrap();
    let result = project.next().unwrap().unwrap();
    
    assert_eq!(result.len(), 2);
    assert_eq!(result.get("id").unwrap(), b"1");
    assert_eq!(result.get("name").unwrap(), b"Alice");
    assert!(result.get("age").is_none());
}

#[test]
fn test_nested_loop_join_integration() {
    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, rustgres::executor::ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            Ok(())
        }
    }

    let mut l1 = HashMap::new();
    l1.insert("user_id".to_string(), b"1".to_vec());
    let mut l2 = HashMap::new();
    l2.insert("user_id".to_string(), b"2".to_vec());

    let mut r1 = HashMap::new();
    r1.insert("order_id".to_string(), b"100".to_vec());
    let mut r2 = HashMap::new();
    r2.insert("order_id".to_string(), b"200".to_vec());

    let left = MockExecutor::new(vec![l1, l2]);
    let right = MockExecutor::new(vec![r1, r2]);

    let mut join = NestedLoopJoin::new(Box::new(left), Box::new(right));
    join.open().unwrap();

    let mut count = 0;
    while let Some(tuple) = join.next().unwrap() {
        assert!(tuple.contains_key("user_id"));
        assert!(tuple.contains_key("order_id"));
        count += 1;
    }

    assert_eq!(count, 4); // 2 x 2 = 4 combinations
}

#[test]
fn test_pipeline_integration() {
    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new() -> Self {
            let mut t1 = HashMap::new();
            t1.insert("id".to_string(), b"1".to_vec());
            t1.insert("name".to_string(), b"Alice".to_vec());
            t1.insert("age".to_string(), b"25".to_vec());
            
            let mut t2 = HashMap::new();
            t2.insert("id".to_string(), b"2".to_vec());
            t2.insert("name".to_string(), b"Bob".to_vec());
            t2.insert("age".to_string(), b"35".to_vec());
            
            let mut t3 = HashMap::new();
            t3.insert("id".to_string(), b"3".to_vec());
            t3.insert("name".to_string(), b"Charlie".to_vec());
            t3.insert("age".to_string(), b"45".to_vec());
            
            Self { tuples: vec![t1, t2, t3], index: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, rustgres::executor::ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), rustgres::executor::ExecutorError> {
            Ok(())
        }
    }

    // SELECT name FROM table WHERE age = 30
    let scan = MockExecutor::new();
    
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Column("age".to_string())),
        op: rustgres::parser::BinaryOperator::Equals,
        right: Box::new(Expr::Number(35)),
    };
    let filter = Filter::new(Box::new(scan), predicate);
    
    let mut project = Project::new(Box::new(filter), vec!["name".to_string()]);
    
    project.open().unwrap();
    
    let result = project.next().unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().get("name").unwrap(), b"Bob");
    
    assert!(project.next().unwrap().is_none());
}
