#[cfg(test)]
mod tests {
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::Statement;

    #[test]
    fn test_parse_recursive_cte() {
        let sql = "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(w) => {
                assert!(w.recursive);
                assert_eq!(w.ctes.len(), 1);
                assert_eq!(w.ctes[0].name, "t");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_non_recursive_cte() {
        let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(w) => {
                assert!(!w.recursive);
                assert_eq!(w.ctes.len(), 1);
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_recursive_cte_multiple() {
        let sql = "WITH RECURSIVE t1 AS (SELECT 1), t2 AS (SELECT 2) SELECT * FROM t1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(w) => {
                assert!(w.recursive);
                assert_eq!(w.ctes.len(), 2);
                assert_eq!(w.ctes[0].name, "t1");
                assert_eq!(w.ctes[1].name, "t2");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_recursive_cte_executor_simple() {
        use vaultgres::catalog::Value;
        use vaultgres::executor::RecursiveCTEExecutor;

        let base = vec![vec![Value::Int(1)]];
        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let Value::Int(n) = row[0] {
                    if n < 5 {
                        results.push(vec![Value::Int(n + 1)]);
                    }
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_recursive_cte_executor_with_cycle() {
        use vaultgres::catalog::Value;
        use vaultgres::executor::RecursiveCTEExecutor;

        let base = vec![vec![Value::Int(1)]];
        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let Value::Int(n) = row[0] {
                    results.push(vec![Value::Int((n % 3) + 1)]);
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_recursive_cte_executor_empty_base() {
        use vaultgres::catalog::Value;
        use vaultgres::executor::RecursiveCTEExecutor;

        let base: Vec<Vec<Value>> = vec![];
        let recursive_fn =
            |_: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(1)]]) };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_recursive_cte_executor_no_recursion() {
        use vaultgres::catalog::Value;
        use vaultgres::executor::RecursiveCTEExecutor;

        let base = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
        let recursive_fn = |_: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![]) };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_recursive_cte_executor_tree_traversal() {
        use vaultgres::catalog::Value;
        use vaultgres::executor::RecursiveCTEExecutor;

        let base = vec![vec![Value::Int(1), Value::Int(0)]];
        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let (Value::Int(id), Value::Int(depth)) = (&row[0], &row[1]) {
                    if *depth < 3 {
                        results.push(vec![Value::Int(id * 2), Value::Int(depth + 1)]);
                        results.push(vec![Value::Int(id * 2 + 1), Value::Int(depth + 1)]);
                    }
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 15);
    }

    #[test]
    fn test_recursive_cte_executor_fibonacci() {
        use vaultgres::catalog::Value;
        use vaultgres::executor::RecursiveCTEExecutor;

        let base = vec![vec![Value::Int(0), Value::Int(1)]];
        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let (Value::Int(a), Value::Int(b)) = (&row[0], &row[1]) {
                    if *b < 100 {
                        results.push(vec![Value::Int(*b), Value::Int(a + b)]);
                    }
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert!(result.len() > 5);
    }
}
