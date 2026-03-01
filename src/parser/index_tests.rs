#[cfg(test)]
mod tests {
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_create_index_basic() {
        let mut parser = Parser::new("CREATE INDEX idx ON users (id)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.name, "idx");
                assert_eq!(idx.table, "users");
                assert_eq!(idx.columns, vec!["id"]);
                assert!(!idx.unique);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_create_unique_index() {
        let mut parser = Parser::new("CREATE UNIQUE INDEX idx ON users (email)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.name, "idx");
                assert!(idx.unique);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_create_index_multiple_columns() {
        let mut parser = Parser::new("CREATE INDEX idx ON users (first_name, last_name)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.columns.len(), 2);
                assert_eq!(idx.columns[0], "first_name");
                assert_eq!(idx.columns[1], "last_name");
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_drop_index_basic() {
        let mut parser = Parser::new("DROP INDEX idx").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropIndex(d) => {
                assert_eq!(d.name, "idx");
                assert!(!d.if_exists);
            }
            _ => panic!("Expected DROP INDEX"),
        }
    }

    #[test]
    fn test_drop_index_if_exists() {
        let mut parser = Parser::new("DROP INDEX IF EXISTS idx").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropIndex(d) => {
                assert_eq!(d.name, "idx");
                assert!(d.if_exists);
            }
            _ => panic!("Expected DROP INDEX"),
        }
    }

    #[test]
    fn test_create_index_three_columns() {
        let mut parser = Parser::new("CREATE INDEX idx ON t (a, b, c)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.columns.len(), 3);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_create_unique_index_multiple_columns() {
        let mut parser = Parser::new("CREATE UNIQUE INDEX idx ON t (a, b)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert!(idx.unique);
                assert_eq!(idx.columns.len(), 2);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }
}
