// E2E tests for parser and SQL features
use vaultgres::parser::ast::Statement;
use vaultgres::parser::Parser;

#[test]
fn test_join_parsing() {
    let sql = "SELECT * FROM users JOIN orders ON id = user_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::Select(s) => assert_eq!(s.joins.len(), 1),
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_union_parsing() {
    let sql = "SELECT id FROM users UNION SELECT id FROM orders";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::Union(u) => assert!(!u.all),
        _ => panic!("Expected UNION"),
    }
}

#[test]
fn test_intersect_parsing() {
    let sql = "SELECT id FROM users INTERSECT SELECT id FROM orders";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::Intersect(_) => {}
        _ => panic!("Expected INTERSECT"),
    }
}

#[test]
fn test_except_parsing() {
    let sql = "SELECT id FROM users EXCEPT SELECT id FROM orders";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::Except(_) => {}
        _ => panic!("Expected EXCEPT"),
    }
}
