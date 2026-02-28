#[cfg(test)]
mod tests {
    use crate::parser::{Parser, Statement};
    use crate::parser::ast::{CreateViewStmt, DropViewStmt, Expr};

    #[test]
    fn test_create_view_basic() {
        let mut parser = Parser::new("CREATE VIEW v AS SELECT * FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.name, "v");
                assert_eq!(v.query.from, "t");
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_create_view_with_where() {
        let mut parser = Parser::new("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.name, "active_users");
                assert!(v.query.where_clause.is_some());
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_create_view_with_columns() {
        let mut parser = Parser::new("CREATE VIEW user_names AS SELECT id, name FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.name, "user_names");
                assert_eq!(v.query.columns.len(), 2);
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_create_view_with_join() {
        let mut parser = Parser::new("CREATE VIEW user_orders AS SELECT * FROM users INNER JOIN orders ON id = user_id").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.name, "user_orders");
                assert_eq!(v.query.joins.len(), 1);
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_create_view_with_aggregate() {
        let mut parser = Parser::new("CREATE VIEW user_count AS SELECT COUNT(*) FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.name, "user_count");
                match &v.query.columns[0] {
                    Expr::Aggregate { .. } => {},
                    _ => panic!("Expected aggregate"),
                }
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }

    #[test]
    fn test_drop_view_basic() {
        let mut parser = Parser::new("DROP VIEW v").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropView(d) => {
                assert_eq!(d.name, "v");
                assert!(!d.if_exists);
            }
            _ => panic!("Expected DROP VIEW"),
        }
    }

    #[test]
    fn test_drop_view_if_exists() {
        let mut parser = Parser::new("DROP VIEW IF EXISTS v").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropView(d) => {
                assert_eq!(d.name, "v");
                assert!(d.if_exists);
            }
            _ => panic!("Expected DROP VIEW"),
        }
    }
}
