#[cfg(test)]
mod tests {
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_view_name_with_underscores() {
        let mut parser = Parser::new("CREATE VIEW my_view_123 AS SELECT * FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => assert_eq!(v.name, "my_view_123"),
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_limit_offset() {
        let mut parser = Parser::new("CREATE VIEW top_users AS SELECT * FROM users LIMIT 10 OFFSET 5").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.query.limit, Some(10));
                assert_eq!(v.query.offset, Some(5));
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_order_by() {
        let mut parser = Parser::new("CREATE VIEW sorted_users AS SELECT * FROM users ORDER BY name ASC").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert!(v.query.order_by.is_some());
                assert_eq!(v.query.order_by.unwrap()[0].column, "name");
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_group_by() {
        let mut parser = Parser::new("CREATE VIEW user_counts AS SELECT dept, COUNT(*) FROM users GROUP BY dept").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert!(v.query.group_by.is_some());
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_having() {
        let mut parser = Parser::new("CREATE VIEW large_depts AS SELECT dept, COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > 10").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert!(v.query.having.is_some());
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_distinct() {
        let mut parser = Parser::new("CREATE VIEW unique_depts AS SELECT DISTINCT dept FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert!(v.query.distinct);
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_multiple_joins() {
        let mut parser = Parser::new("CREATE VIEW full_data AS SELECT * FROM a INNER JOIN b ON a_id = b_id LEFT JOIN c ON b_id = c_id").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.query.joins.len(), 2);
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_complex_where() {
        let mut parser = Parser::new("CREATE VIEW filtered AS SELECT * FROM t WHERE a > 10 AND b < 20 OR c = 30").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert!(v.query.where_clause.is_some());
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_drop_view_case_insensitive() {
        let mut parser = Parser::new("drop view MyView").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropView(d) => assert_eq!(d.name, "MyView"),
            _ => panic!("Expected DROP VIEW"),
        }
    }

    #[test]
    fn test_view_with_all_aggregates() {
        let mut parser = Parser::new("CREATE VIEW stats AS SELECT COUNT(*), SUM(x), AVG(y), MIN(z), MAX(w) FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.query.columns.len(), 5);
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_string_literals() {
        let mut parser = Parser::new("CREATE VIEW filtered AS SELECT * FROM t WHERE name = 'test'").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(_) => {},
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_numeric_literals() {
        let mut parser = Parser::new("CREATE VIEW filtered AS SELECT * FROM t WHERE id = 123").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(_) => {},
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_view_with_comparison_operators() {
        let mut parser = Parser::new("CREATE VIEW filtered AS SELECT * FROM t WHERE a < 10 AND b <= 20 AND c > 30 AND d >= 40 AND e != 50").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(_) => {},
            _ => panic!("Expected CREATE VIEW"),
        }
    }
}
