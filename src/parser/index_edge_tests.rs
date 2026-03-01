#[cfg(test)]
mod tests {
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_index_name_variations() {
        let names = vec!["idx1", "my_index", "index_123", "MixedCase"];
        
        for name in names {
            let sql = format!("CREATE INDEX {} ON t (id)", name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            
            match stmt {
                Statement::CreateIndex(idx) => {
                    assert_eq!(idx.name, name);
                }
                _ => panic!("Expected CREATE INDEX"),
            }
        }
    }

    #[test]
    fn test_index_table_variations() {
        let tables = vec!["users", "orders", "products_123"];
        
        for table in tables {
            let sql = format!("CREATE INDEX idx ON {} (id)", table);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            
            match stmt {
                Statement::CreateIndex(idx) => {
                    assert_eq!(idx.table, table);
                }
                _ => panic!("Expected CREATE INDEX"),
            }
        }
    }

    #[test]
    fn test_index_column_variations() {
        let columns = vec!["id", "user_id", "created_at", "status"];
        
        for column in columns {
            let sql = format!("CREATE INDEX idx ON t ({})", column);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            
            match stmt {
                Statement::CreateIndex(idx) => {
                    assert_eq!(idx.columns[0], column);
                }
                _ => panic!("Expected CREATE INDEX"),
            }
        }
    }

    #[test]
    fn test_index_case_insensitive() {
        let mut parser = Parser::new("create index MyIndex on MyTable (MyColumn)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.name, "MyIndex");
                assert_eq!(idx.table, "MyTable");
                assert_eq!(idx.columns[0], "MyColumn");
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_unique_index_case_insensitive() {
        let mut parser = Parser::new("create unique index idx on t (id)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert!(idx.unique);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_drop_index_case_insensitive() {
        let mut parser = Parser::new("drop index MyIndex").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropIndex(d) => {
                assert_eq!(d.name, "MyIndex");
            }
            _ => panic!("Expected DROP INDEX"),
        }
    }

    #[test]
    fn test_index_four_columns() {
        let mut parser = Parser::new("CREATE INDEX idx ON t (a, b, c, d)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.columns.len(), 4);
                assert_eq!(idx.columns, vec!["a", "b", "c", "d"]);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_index_five_columns() {
        let mut parser = Parser::new("CREATE INDEX idx ON t (a, b, c, d, e)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert_eq!(idx.columns.len(), 5);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_unique_index_single_column() {
        let mut parser = Parser::new("CREATE UNIQUE INDEX idx ON users (email)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert!(idx.unique);
                assert_eq!(idx.columns.len(), 1);
                assert_eq!(idx.columns[0], "email");
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_unique_index_three_columns() {
        let mut parser = Parser::new("CREATE UNIQUE INDEX idx ON t (a, b, c)").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateIndex(idx) => {
                assert!(idx.unique);
                assert_eq!(idx.columns.len(), 3);
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    #[test]
    fn test_drop_index_if_exists_case_insensitive() {
        let mut parser = Parser::new("drop index if exists MyIndex").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::DropIndex(d) => {
                assert_eq!(d.name, "MyIndex");
                assert!(d.if_exists);
            }
            _ => panic!("Expected DROP INDEX"),
        }
    }
}
