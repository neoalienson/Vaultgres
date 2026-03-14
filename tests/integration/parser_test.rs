use vaultgres::parser::{Expr, Parser, Statement, parse};

fn parse_select(sql: &str) -> vaultgres::parser::ast::SelectStmt {
    match parse(sql).unwrap() {
        Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    }
}

fn parse_insert(sql: &str) -> vaultgres::parser::ast::InsertStmt {
    match parse(sql).unwrap() {
        Statement::Insert(s) => s,
        _ => panic!("Expected INSERT statement"),
    }
}

fn parse_update(sql: &str) -> vaultgres::parser::ast::UpdateStmt {
    match parse(sql).unwrap() {
        Statement::Update(s) => s,
        _ => panic!("Expected UPDATE statement"),
    }
}

fn parse_delete(sql: &str) -> vaultgres::parser::ast::DeleteStmt {
    match parse(sql).unwrap() {
        Statement::Delete(s) => s,
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_simple_select() {
    let s = parse_select("SELECT * FROM users");
    assert_eq!(s.from, "users");
    assert_eq!(s.columns, vec![Expr::Star]);
    assert!(s.where_clause.is_none());
}

#[test]
fn test_parse_select_with_columns() {
    let s = parse_select("SELECT id, name FROM users");
    assert_eq!(s.from, "users");
    assert_eq!(s.columns.len(), 2);
}

#[test]
fn test_parse_select_with_where() {
    let s = parse_select("SELECT * FROM users WHERE id = 1");
    assert_eq!(s.from, "users");
    assert!(s.where_clause.is_some());
}

#[test]
fn test_parse_insert() {
    let s = parse_insert("INSERT INTO users VALUES (1, 'Alice')");
    assert_eq!(s.table, "users");
    assert_eq!(s.values.len(), 2);
    assert_eq!(s.values[0], Expr::Number(1));
    assert_eq!(s.values[1], Expr::String("Alice".to_string()));
}

#[test]
fn test_parse_update() {
    let s = parse_update("UPDATE users SET name = 'Bob'");
    assert_eq!(s.table, "users");
    assert_eq!(s.assignments.len(), 1);
    assert_eq!(s.assignments[0].0, "name");
}

#[test]
fn test_parse_update_with_where() {
    let s = parse_update("UPDATE users SET name = 'Bob' WHERE id = 1");
    assert_eq!(s.table, "users");
    assert!(s.where_clause.is_some());
}

#[test]
fn test_parse_delete() {
    let s = parse_delete("DELETE FROM users");
    assert_eq!(s.table, "users");
    assert!(s.where_clause.is_none());
}

#[test]
fn test_parse_delete_with_where() {
    let s = parse_delete("DELETE FROM users WHERE id = 1");
    assert_eq!(s.table, "users");
    assert!(s.where_clause.is_some());
}

#[test]
fn test_parse_multiple_assignments() {
    let s = parse_update("UPDATE users SET name = 'Bob', age = 30");
    assert_eq!(s.assignments.len(), 2);
    assert_eq!(s.assignments[0].0, "name");
    assert_eq!(s.assignments[1].0, "age");
}

#[test]
fn test_parse_case_insensitive() {
    let stmt1 = parse("select * from users").unwrap();
    let stmt2 = parse("SELECT * FROM users").unwrap();
    assert_eq!(stmt1, stmt2);
}

#[test]
fn test_parse_error_invalid_syntax() {
    assert!(parse("SELECT FROM").is_err());
}

#[test]
fn test_parse_error_unexpected_token() {
    assert!(parse("INVALID STATEMENT").is_err());
}

#[test]
fn test_parse_select_without_from() {
    let s = parse_select("SELECT 1");
    assert_eq!(s.from, "");
    assert_eq!(s.columns.len(), 1);
}

#[test]
fn test_parse_select_multiple_columns() {
    let s = parse_select("SELECT id, name, email FROM users");
    assert_eq!(s.columns.len(), 3);
    assert_eq!(s.columns[0], Expr::Column("id".to_string()));
    assert_eq!(s.columns[1], Expr::Column("name".to_string()));
    assert_eq!(s.columns[2], Expr::Column("email".to_string()));
}

#[test]
fn test_parse_insert_multiple_values() {
    let s = parse_insert("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    assert_eq!(s.values.len(), 3);
    assert_eq!(s.values[0], Expr::Number(1));
    assert_eq!(s.values[1], Expr::String("Alice".to_string()));
    assert_eq!(s.values[2], Expr::String("alice@example.com".to_string()));
}

#[test]
fn test_parse_update_multiple_assignments() {
    let s = parse_update("UPDATE users SET name = 'Bob', age = 30, email = 'bob@example.com'");
    assert_eq!(s.assignments.len(), 3);
    assert_eq!(s.assignments[0].0, "name");
    assert_eq!(s.assignments[1].0, "age");
    assert_eq!(s.assignments[2].0, "email");
}

#[test]
fn test_parse_with_semicolon() {
    let s = parse_select("SELECT * FROM users;");
    assert_eq!(s.from, "users");
}

#[test]
fn test_parse_select_number() {
    let s = parse_select("SELECT 42");
    assert_eq!(s.columns.len(), 1);
    assert_eq!(s.columns[0], Expr::Number(42));
}

#[test]
fn test_parse_select_string() {
    let s = parse_select("SELECT 'hello world'");
    assert_eq!(s.columns.len(), 1);
    assert_eq!(s.columns[0], Expr::String("hello world".to_string()));
}

#[test]
fn test_parse_where_equals() {
    let s = parse_select("SELECT * FROM users WHERE id = 1");
    assert!(s.where_clause.is_some());
    match s.where_clause.unwrap() {
        Expr::BinaryOp { left, op: _, right } => {
            assert_eq!(*left, Expr::Column("id".to_string()));
            assert_eq!(*right, Expr::Number(1));
        }
        _ => panic!("Expected binary op"),
    }
}

#[test]
fn test_parse_empty_string() {
    assert!(parse("").is_err());
}

#[test]
fn test_parse_whitespace_only() {
    assert!(parse("   \n\t  ").is_err());
}

#[test]
fn test_join_parsing() {
    let s = parse_select("SELECT * FROM users JOIN orders ON id = user_id");
    assert_eq!(s.joins.len(), 1);
}

#[test]
fn test_union_parsing() {
    let mut parser = Parser::new("SELECT id FROM users UNION SELECT id FROM orders").unwrap();
    match parser.parse().unwrap() {
        Statement::Union(u) => assert!(!u.all),
        _ => panic!("Expected UNION"),
    }
}

#[test]
fn test_intersect_parsing() {
    let mut parser = Parser::new("SELECT id FROM users INTERSECT SELECT id FROM orders").unwrap();
    assert!(matches!(parser.parse().unwrap(), Statement::Intersect(_)));
}

#[test]
fn test_except_parsing() {
    let mut parser = Parser::new("SELECT id FROM users EXCEPT SELECT id FROM orders").unwrap();
    assert!(matches!(parser.parse().unwrap(), Statement::Except(_)));
}
