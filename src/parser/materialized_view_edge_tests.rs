#[cfg(test)]
mod tests {
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_materialized_view_with_distinct() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW unique_mv AS SELECT DISTINCT dept FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert!(mv.query.distinct);
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_order_by() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW sorted_mv AS SELECT * FROM t ORDER BY name ASC").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert!(mv.query.order_by.is_some());
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_limit() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW top10_mv AS SELECT * FROM t LIMIT 10").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.query.limit, Some(10));
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_offset() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW offset_mv AS SELECT * FROM t OFFSET 5").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.query.offset, Some(5));
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_group_by() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW grouped_mv AS SELECT dept, COUNT(*) FROM users GROUP BY dept").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert!(mv.query.group_by.is_some());
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_having() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW filtered_mv AS SELECT dept, COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > 5").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert!(mv.query.having.is_some());
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_complex_query() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW complex_mv AS SELECT dept, COUNT(*), AVG(salary) FROM employees WHERE active = 1 GROUP BY dept HAVING COUNT(*) > 10 ORDER BY dept LIMIT 20").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert!(mv.query.where_clause.is_some());
                assert!(mv.query.group_by.is_some());
                assert!(mv.query.having.is_some());
                assert!(mv.query.order_by.is_some());
                assert_eq!(mv.query.limit, Some(20));
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_multiple_aggregates() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW agg_mv AS SELECT COUNT(*), SUM(x), AVG(y), MIN(z), MAX(w) FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.query.columns.len(), 5);
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_name_variations() {
        let names = vec!["mv1", "my_view", "view_123", "MixedCase"];
        
        for name in names {
            let sql = format!("CREATE MATERIALIZED VIEW {} AS SELECT * FROM t", name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            
            match stmt {
                Statement::CreateMaterializedView(mv) => {
                    assert_eq!(mv.name, name);
                }
                _ => panic!("Expected CREATE MATERIALIZED VIEW"),
            }
        }
    }

    #[test]
    fn test_refresh_case_insensitive() {
        let mut parser = Parser::new("refresh materialized view MyView").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert_eq!(r.name, "MyView");
            }
            _ => panic!("Expected REFRESH MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_drop_case_insensitive() {
        let mut parser = Parser::new("drop materialized view MyView").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropMaterializedView(d) => {
                assert_eq!(d.name, "MyView");
            }
            _ => panic!("Expected DROP MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_multiple_joins() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW multi_join_mv AS SELECT * FROM a INNER JOIN b ON a_id = b_id LEFT JOIN c ON b_id = c_id").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.query.joins.len(), 2);
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }
}
