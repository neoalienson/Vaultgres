#[cfg(test)]
mod tests {
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_create_materialized_view_basic() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.name, "mv");
                assert_eq!(mv.query.from, "t");
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_create_materialized_view_with_where() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW active_mv AS SELECT * FROM users WHERE active = 1").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.name, "active_mv");
                assert!(mv.query.where_clause.is_some());
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_create_materialized_view_with_aggregate() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW stats_mv AS SELECT COUNT(*) FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(_) => {},
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_refresh_materialized_view() {
        let mut parser = Parser::new("REFRESH MATERIALIZED VIEW mv").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert_eq!(r.name, "mv");
            }
            _ => panic!("Expected REFRESH MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_drop_materialized_view_basic() {
        let mut parser = Parser::new("DROP MATERIALIZED VIEW mv").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropMaterializedView(d) => {
                assert_eq!(d.name, "mv");
                assert!(!d.if_exists);
            }
            _ => panic!("Expected DROP MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_drop_materialized_view_if_exists() {
        let mut parser = Parser::new("DROP MATERIALIZED VIEW IF EXISTS mv").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropMaterializedView(d) => {
                assert_eq!(d.name, "mv");
                assert!(d.if_exists);
            }
            _ => panic!("Expected DROP MATERIALIZED VIEW"),
        }
    }

    #[test]
    fn test_materialized_view_with_join() {
        let mut parser = Parser::new("CREATE MATERIALIZED VIEW joined_mv AS SELECT * FROM a INNER JOIN b ON id = bid").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateMaterializedView(mv) => {
                assert_eq!(mv.query.joins.len(), 1);
            }
            _ => panic!("Expected CREATE MATERIALIZED VIEW"),
        }
    }
}
