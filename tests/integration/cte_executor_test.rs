#[cfg(test)]
mod tests {
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::Statement;

    #[test]
    fn test_parse_simple_cte() {
        let sql = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert_eq!(with_stmt.ctes[0].name, "cte");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_with_multiple_columns() {
        let sql = "WITH cte AS (SELECT id, name, value FROM table1) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert_eq!(with_stmt.ctes[0].name, "cte");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_multiple_ctes() {
        let sql = "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 2);
                assert_eq!(with_stmt.ctes[0].name, "cte1");
                assert_eq!(with_stmt.ctes[1].name, "cte2");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_with_where() {
        let sql = "WITH cte AS (SELECT id FROM test_table WHERE value > 10) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert!(with_stmt.ctes[0].query.where_clause.is_some());
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_with_join() {
        let sql =
            "WITH cte AS (SELECT a.id, b.value FROM a JOIN b ON a.id = b.id) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert_eq!(with_stmt.ctes[0].name, "cte");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_with_group_by() {
        let sql = "WITH cte AS (SELECT category, COUNT(*) as cnt FROM products GROUP BY category) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert!(with_stmt.ctes[0].query.group_by.is_some());
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_with_order_by() {
        let sql = "WITH cte AS (SELECT id FROM table1 ORDER BY id DESC) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert!(with_stmt.ctes[0].query.order_by.is_some());
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_with_limit() {
        let sql = "WITH cte AS (SELECT id FROM table1 LIMIT 10) SELECT * FROM cte";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert!(with_stmt.ctes[0].query.limit.is_some());
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_subquery_in_where() {
        let sql = "WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 50000) SELECT * FROM high_earners WHERE name LIKE 'J%'";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert_eq!(with_stmt.ctes[0].name, "high_earners");
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_for_deduplication() {
        let sql = "WITH unique_values AS (SELECT DISTINCT category FROM products) SELECT * FROM unique_values";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 1);
                assert!(with_stmt.ctes[0].query.distinct);
            }
            _ => panic!("Expected WITH statement"),
        }
    }

    #[test]
    fn test_parse_cte_referencing_earlier_cte() {
        let sql = "WITH cte1 AS (SELECT id FROM table1), cte2 AS (SELECT id FROM cte1 WHERE id > 10) SELECT * FROM cte2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::With(with_stmt) => {
                assert!(!with_stmt.recursive);
                assert_eq!(with_stmt.ctes.len(), 2);
                assert_eq!(with_stmt.ctes[0].name, "cte1");
                assert_eq!(with_stmt.ctes[1].name, "cte2");
            }
            _ => panic!("Expected WITH statement"),
        }
    }
}
