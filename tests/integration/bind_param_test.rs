#[cfg(test)]
mod tests {
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::{Expr, Statement};

    #[test]
    fn test_parse_parameter_in_where() {
        let sql = "SELECT * FROM users WHERE id = $1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
                match s.where_clause.unwrap() {
                    Expr::BinaryOp { right, .. } => match *right {
                        Expr::Parameter(1) => {}
                        _ => panic!("Expected Parameter(1)"),
                    },
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_multiple_parameters() {
        let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_parameter_in_insert() {
        let sql = "INSERT INTO users VALUES ($1, $2)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.values.len(), 2);
                match &i.values[0] {
                    Expr::Parameter(1) => {}
                    _ => panic!("Expected Parameter(1)"),
                }
                match &i.values[1] {
                    Expr::Parameter(2) => {}
                    _ => panic!("Expected Parameter(2)"),
                }
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_parameter_in_update() {
        let sql = "UPDATE users SET name = $1 WHERE id = $2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.assignments.len(), 1);
                match &u.assignments[0].1 {
                    Expr::Parameter(1) => {}
                    _ => panic!("Expected Parameter(1)"),
                }
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_parameter_in_select_list() {
        let sql = "SELECT $1 FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                match &s.columns[0] {
                    Expr::Parameter(1) => {}
                    _ => panic!("Expected Parameter(1)"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_prepare_with_parameters() {
        let sql = "PREPARE stmt AS SELECT * FROM users WHERE id = $1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Prepare(p) => {
                assert_eq!(p.name, "stmt");
                match *p.statement {
                    Statement::Select(s) => {
                        assert!(s.where_clause.is_some());
                    }
                    _ => panic!("Expected SELECT statement"),
                }
            }
            _ => panic!("Expected PREPARE statement"),
        }
    }

    #[test]
    fn test_parameter_numbers() {
        let sql = "SELECT $1, $2, $10 FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 3);
                match &s.columns[0] {
                    Expr::Parameter(1) => {}
                    _ => panic!("Expected Parameter(1)"),
                }
                match &s.columns[1] {
                    Expr::Parameter(2) => {}
                    _ => panic!("Expected Parameter(2)"),
                }
                match &s.columns[2] {
                    Expr::Parameter(10) => {}
                    _ => panic!("Expected Parameter(10)"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parameter_in_comparison() {
        let sql = "SELECT * FROM users WHERE age > $1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
                match s.where_clause.unwrap() {
                    Expr::BinaryOp { right, .. } => match *right {
                        Expr::Parameter(1) => {}
                        _ => panic!("Expected Parameter(1)"),
                    },
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parameter_in_in_clause() {
        let sql = "SELECT * FROM users WHERE id IN ($1, $2, $3)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_mixed_parameters_and_literals() {
        let sql = "SELECT * FROM users WHERE id = $1 AND status = 'active'";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parameter_in_delete() {
        let sql = "DELETE FROM users WHERE id = $1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Delete(d) => {
                assert!(d.where_clause.is_some());
                match d.where_clause.unwrap() {
                    Expr::BinaryOp { right, .. } => match *right {
                        Expr::Parameter(1) => {}
                        _ => panic!("Expected Parameter(1)"),
                    },
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected DELETE statement"),
        }
    }
}
