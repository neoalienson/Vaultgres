#[cfg(test)]
mod tests {
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::Statement;

    #[test]
    fn test_parse_lateral_join() {
        let sql = "SELECT * FROM t1 LATERAL JOIN t2 ON id = id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.joins.len(), 1);
                assert!(s.joins[0].lateral);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_lateral_left_join() {
        let sql = "SELECT * FROM t1 LATERAL LEFT JOIN t2 ON id = id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.joins.len(), 1);
                assert!(s.joins[0].lateral);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_non_lateral_join() {
        let sql = "SELECT * FROM t1 JOIN t2 ON id = id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.joins.len(), 1);
                assert!(!s.joins[0].lateral);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_lateral_executor_simple() {
        use std::collections::HashMap;
        use vaultgres::catalog::Value;
        use vaultgres::executor::LateralSubqueryExecutor;

        let mut outer1 = HashMap::new();
        outer1.insert("id".to_string(), Value::Int(1));

        let mut outer2 = HashMap::new();
        outer2.insert("id".to_string(), Value::Int(2));

        let outer_rows = vec![outer1, outer2];

        let subquery_fn = |outer: &HashMap<String, Value>| {
            let id = match outer.get("id") {
                Some(Value::Int(n)) => *n,
                _ => return Err("Invalid id".to_string()),
            };

            let mut row = HashMap::new();
            row.insert("value".to_string(), Value::Int(id * 10));
            Ok(vec![row])
        };

        let results = LateralSubqueryExecutor::execute(outer_rows, subquery_fn).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("value"), Some(&Value::Int(10)));
        assert_eq!(results[1].get("value"), Some(&Value::Int(20)));
    }

    #[test]
    fn test_lateral_executor_multiple_results() {
        use std::collections::HashMap;
        use vaultgres::catalog::Value;
        use vaultgres::executor::LateralSubqueryExecutor;

        let mut outer = HashMap::new();
        outer.insert("id".to_string(), Value::Int(1));

        let subquery_fn = |_: &HashMap<String, Value>| {
            let mut row1 = HashMap::new();
            row1.insert("value".to_string(), Value::Int(10));

            let mut row2 = HashMap::new();
            row2.insert("value".to_string(), Value::Int(20));

            Ok(vec![row1, row2])
        };

        let results = LateralSubqueryExecutor::execute(vec![outer], subquery_fn).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_lateral_executor_empty_subquery() {
        use std::collections::HashMap;
        use vaultgres::catalog::Value;
        use vaultgres::executor::LateralSubqueryExecutor;

        let mut outer = HashMap::new();
        outer.insert("id".to_string(), Value::Int(1));

        let subquery_fn = |_: &HashMap<String, Value>| Ok(vec![]);

        let results = LateralSubqueryExecutor::execute(vec![outer], subquery_fn).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_lateral_executor_preserves_outer_columns() {
        use std::collections::HashMap;
        use vaultgres::catalog::Value;
        use vaultgres::executor::LateralSubqueryExecutor;

        let mut outer = HashMap::new();
        outer.insert("id".to_string(), Value::Int(1));
        outer.insert("name".to_string(), Value::Text("Alice".to_string()));

        let subquery_fn = |_: &HashMap<String, Value>| {
            let mut row = HashMap::new();
            row.insert("value".to_string(), Value::Int(100));
            Ok(vec![row])
        };

        let results = LateralSubqueryExecutor::execute(vec![outer], subquery_fn).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(results[0].get("value"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_lateral_executor_dependent_subquery() {
        use std::collections::HashMap;
        use vaultgres::catalog::Value;
        use vaultgres::executor::LateralSubqueryExecutor;

        let mut outer1 = HashMap::new();
        outer1.insert("limit".to_string(), Value::Int(2));

        let mut outer2 = HashMap::new();
        outer2.insert("limit".to_string(), Value::Int(3));

        let outer_rows = vec![outer1, outer2];

        let subquery_fn = |outer: &HashMap<String, Value>| {
            let limit = match outer.get("limit") {
                Some(Value::Int(n)) => *n,
                _ => return Err("Invalid limit".to_string()),
            };

            let mut results = Vec::new();
            for i in 1..=limit {
                let mut row = HashMap::new();
                row.insert("num".to_string(), Value::Int(i));
                results.push(row);
            }
            Ok(results)
        };

        let results = LateralSubqueryExecutor::execute(outer_rows, subquery_fn).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_lateral_executor_cross_product() {
        use std::collections::HashMap;
        use vaultgres::catalog::Value;
        use vaultgres::executor::LateralSubqueryExecutor;

        let mut outer1 = HashMap::new();
        outer1.insert("x".to_string(), Value::Int(1));

        let mut outer2 = HashMap::new();
        outer2.insert("x".to_string(), Value::Int(2));

        let outer_rows = vec![outer1, outer2];

        let subquery_fn = |_: &HashMap<String, Value>| {
            let mut row1 = HashMap::new();
            row1.insert("y".to_string(), Value::Int(10));

            let mut row2 = HashMap::new();
            row2.insert("y".to_string(), Value::Int(20));

            Ok(vec![row1, row2])
        };

        let results = LateralSubqueryExecutor::execute(outer_rows, subquery_fn).unwrap();
        assert_eq!(results.len(), 4);
    }
}
