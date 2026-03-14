#[cfg(test)]
mod tests {
    use vaultgres::PreparedStatementManager;
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::{Expr, Statement};

    #[test]
    fn test_parse_prepare() {
        let sql = "PREPARE test AS SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Prepare(p) => {
                assert_eq!(p.name, "test");
                match *p.statement {
                    Statement::Select(_) => {}
                    _ => panic!("Expected SELECT statement"),
                }
            }
            _ => panic!("Expected PREPARE statement"),
        }
    }

    #[test]
    fn test_parse_execute_no_params() {
        let sql = "EXECUTE test";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Execute(e) => {
                assert_eq!(e.name, "test");
                assert_eq!(e.params.len(), 0);
            }
            _ => panic!("Expected EXECUTE statement"),
        }
    }

    #[test]
    fn test_parse_execute_with_params() {
        let sql = "EXECUTE test(1, 2)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Execute(e) => {
                assert_eq!(e.name, "test");
                assert_eq!(e.params.len(), 2);
            }
            _ => panic!("Expected EXECUTE statement"),
        }
    }

    #[test]
    fn test_parse_deallocate() {
        let sql = "DEALLOCATE test";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Deallocate(name) => {
                assert_eq!(name, "test");
            }
            _ => panic!("Expected DEALLOCATE statement"),
        }
    }

    #[test]
    fn test_prepared_statement_manager() {
        let manager = PreparedStatementManager::new();

        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        manager.prepare("test".to_string(), stmt.clone());

        let retrieved = manager.get("test");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), stmt);
    }

    #[test]
    fn test_prepare_and_execute_workflow() {
        let manager = PreparedStatementManager::new();

        let prepare_sql = "PREPARE get_user AS SELECT * FROM users";
        let mut parser = Parser::new(prepare_sql).unwrap();
        let prepare_stmt = parser.parse().unwrap();

        match prepare_stmt {
            Statement::Prepare(p) => {
                manager.prepare(p.name.clone(), *p.statement);

                let retrieved = manager.get(&p.name);
                assert!(retrieved.is_some());
            }
            _ => panic!("Expected PREPARE statement"),
        }
    }

    #[test]
    fn test_deallocate_workflow() {
        let manager = PreparedStatementManager::new();

        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        manager.prepare("test".to_string(), stmt);
        assert!(manager.get("test").is_some());

        assert!(manager.deallocate("test"));
        assert!(manager.get("test").is_none());
    }

    #[test]
    fn test_multiple_prepared_statements() {
        let manager = PreparedStatementManager::new();

        let sql1 = "SELECT * FROM users";
        let mut parser1 = Parser::new(sql1).unwrap();
        let stmt1 = parser1.parse().unwrap();

        let sql2 = "SELECT * FROM orders";
        let mut parser2 = Parser::new(sql2).unwrap();
        let stmt2 = parser2.parse().unwrap();

        manager.prepare("stmt1".to_string(), stmt1.clone());
        manager.prepare("stmt2".to_string(), stmt2.clone());

        assert_eq!(manager.get("stmt1").unwrap(), stmt1);
        assert_eq!(manager.get("stmt2").unwrap(), stmt2);
    }

    #[test]
    fn test_prepare_insert_statement() {
        let sql = "PREPARE insert_user AS INSERT INTO users VALUES (1, 2)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Prepare(p) => {
                assert_eq!(p.name, "insert_user");
                match *p.statement {
                    Statement::Insert(_) => {}
                    _ => panic!("Expected INSERT statement"),
                }
            }
            _ => panic!("Expected PREPARE statement"),
        }
    }

    #[test]
    fn test_execute_with_string_params() {
        let sql = "EXECUTE test(1, 'hello')";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Execute(e) => {
                assert_eq!(e.name, "test");
                assert_eq!(e.params.len(), 2);
                match &e.params[0] {
                    Expr::Number(1) => {}
                    _ => panic!("Expected number"),
                }
                match &e.params[1] {
                    Expr::String(s) => assert_eq!(s, "hello"),
                    _ => panic!("Expected string"),
                }
            }
            _ => panic!("Expected EXECUTE statement"),
        }
    }

    #[test]
    fn test_clear_all_statements() {
        let manager = PreparedStatementManager::new();

        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        manager.prepare("stmt1".to_string(), stmt.clone());
        manager.prepare("stmt2".to_string(), stmt);

        manager.clear();

        assert!(manager.get("stmt1").is_none());
        assert!(manager.get("stmt2").is_none());
    }
}
