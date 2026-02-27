use rustgres::parser::{Parser, Statement, Expr};

#[test]
fn test_parse_select_star() {
    let mut parser = Parser::new("SELECT * FROM users").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns, vec![Expr::Star]);
            assert_eq!(s.from, "users");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_select_without_from() {
    let mut parser = Parser::new("SELECT 1").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, "");
            assert_eq!(s.columns.len(), 1);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_select_with_semicolon() {
    let mut parser = Parser::new("SELECT * FROM users;").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => assert_eq!(s.from, "users"),
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_insert_single_value() {
    let mut parser = Parser::new("INSERT INTO users VALUES (1)").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Insert(s) => {
            assert_eq!(s.table, "users");
            assert_eq!(s.values.len(), 1);
        }
        _ => panic!("Expected INSERT"),
    }
}

#[test]
fn test_parse_insert_multiple_values() {
    let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Insert(s) => {
            assert_eq!(s.values.len(), 3);
        }
        _ => panic!("Expected INSERT"),
    }
}

#[test]
fn test_parse_update_single_assignment() {
    let mut parser = Parser::new("UPDATE users SET name = 'Bob'").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Update(s) => {
            assert_eq!(s.assignments.len(), 1);
            assert_eq!(s.assignments[0].0, "name");
        }
        _ => panic!("Expected UPDATE"),
    }
}

#[test]
fn test_parse_update_multiple_assignments() {
    let mut parser = Parser::new("UPDATE users SET name = 'Bob', age = 30").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Update(s) => {
            assert_eq!(s.assignments.len(), 2);
        }
        _ => panic!("Expected UPDATE"),
    }
}

#[test]
fn test_parse_delete_without_where() {
    let mut parser = Parser::new("DELETE FROM users").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Delete(s) => {
            assert_eq!(s.table, "users");
            assert!(s.where_clause.is_none());
        }
        _ => panic!("Expected DELETE"),
    }
}

#[test]
fn test_parse_delete_with_where() {
    let mut parser = Parser::new("DELETE FROM users WHERE id = 1").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Delete(s) => {
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected DELETE"),
    }
}

#[test]
fn test_parse_empty_string() {
    let result = Parser::new("");
    // Empty string creates parser with EOF token
    assert!(result.is_ok());
    
    let mut parser = result.unwrap();
    let result = parser.parse();
    assert!(result.is_err()); // But parsing fails
}

#[test]
fn test_parse_whitespace_only() {
    let result = Parser::new("   \n\t  ");
    // Whitespace only creates parser with EOF token
    assert!(result.is_ok());
    
    let mut parser = result.unwrap();
    let result = parser.parse();
    assert!(result.is_err()); // But parsing fails
}

#[test]
fn test_parse_invalid_keyword() {
    let result = Parser::new("INVALID STATEMENT");
    assert!(result.is_ok()); // Lexer succeeds
    
    let mut parser = result.unwrap();
    let result = parser.parse();
    assert!(result.is_err()); // Parser fails
}

#[test]
fn test_parse_incomplete_select() {
    let result = Parser::new("SELECT");
    assert!(result.is_ok());
    
    let mut parser = result.unwrap();
    let result = parser.parse();
    assert!(result.is_err());
}

#[test]
fn test_parse_incomplete_insert() {
    let result = Parser::new("INSERT INTO");
    assert!(result.is_ok());
    
    let mut parser = result.unwrap();
    let result = parser.parse();
    assert!(result.is_err());
}

#[test]
fn test_parse_case_insensitive_keywords() {
    let queries = vec![
        "select * from users",
        "SELECT * FROM users",
        "SeLeCt * FrOm users",
    ];
    
    for query in queries {
        let mut parser = Parser::new(query).unwrap();
        assert!(parser.parse().is_ok());
    }
}

#[test]
fn test_parse_number_zero() {
    let mut parser = Parser::new("SELECT 0").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns[0], Expr::Number(0));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_large_number() {
    let mut parser = Parser::new("SELECT 999999").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns[0], Expr::Number(999999));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_empty_string_literal() {
    let mut parser = Parser::new("SELECT ''").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns[0], Expr::String("".to_string()));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_string_with_spaces() {
    let mut parser = Parser::new("SELECT 'hello world'").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns[0], Expr::String("hello world".to_string()));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_multiple_columns() {
    let mut parser = Parser::new("SELECT id, name, email FROM users").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 3);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_where_equals() {
    let mut parser = Parser::new("SELECT * FROM users WHERE id = 1").unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}
