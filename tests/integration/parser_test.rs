use vaultgres::parser::{parse, Expr, Statement};

#[test]
fn test_parse_simple_select() {
    let stmt = parse("SELECT * FROM users").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, "users");
            assert_eq!(s.columns, vec![Expr::Star]);
            assert!(s.where_clause.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_columns() {
    let stmt = parse("SELECT id, name FROM users").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, "users");
            assert_eq!(s.columns.len(), 2);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_where() {
    let stmt = parse("SELECT * FROM users WHERE id = 1").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, "users");
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_insert() {
    let stmt = parse("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    match stmt {
        Statement::Insert(s) => {
            assert_eq!(s.table, "users");
            assert_eq!(s.values.len(), 2);
            assert_eq!(s.values[0], Expr::Number(1));
            assert_eq!(s.values[1], Expr::String("Alice".to_string()));
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_update() {
    let stmt = parse("UPDATE users SET name = 'Bob'").unwrap();

    match stmt {
        Statement::Update(s) => {
            assert_eq!(s.table, "users");
            assert_eq!(s.assignments.len(), 1);
            assert_eq!(s.assignments[0].0, "name");
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_update_with_where() {
    let stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();

    match stmt {
        Statement::Update(s) => {
            assert_eq!(s.table, "users");
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_delete() {
    let stmt = parse("DELETE FROM users").unwrap();

    match stmt {
        Statement::Delete(s) => {
            assert_eq!(s.table, "users");
            assert!(s.where_clause.is_none());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_delete_with_where() {
    let stmt = parse("DELETE FROM users WHERE id = 1").unwrap();

    match stmt {
        Statement::Delete(s) => {
            assert_eq!(s.table, "users");
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected DELETE statement"),
    }
}

#[test]
fn test_parse_multiple_assignments() {
    let stmt = parse("UPDATE users SET name = 'Bob', age = 30").unwrap();

    match stmt {
        Statement::Update(s) => {
            assert_eq!(s.assignments.len(), 2);
            assert_eq!(s.assignments[0].0, "name");
            assert_eq!(s.assignments[1].0, "age");
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_case_insensitive() {
    let stmt1 = parse("select * from users").unwrap();
    let stmt2 = parse("SELECT * FROM users").unwrap();

    assert_eq!(stmt1, stmt2);
}

#[test]
fn test_parse_error_invalid_syntax() {
    let result = parse("SELECT FROM");
    assert!(result.is_err());
}

#[test]
fn test_parse_error_unexpected_token() {
    let result = parse("INVALID STATEMENT");
    assert!(result.is_err());
}

#[test]
fn test_parse_select_without_from() {
    let stmt = parse("SELECT 1").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, "");
            assert_eq!(s.columns.len(), 1);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_multiple_columns() {
    let stmt = parse("SELECT id, name, email FROM users").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 3);
            assert_eq!(s.columns[0], Expr::Column("id".to_string()));
            assert_eq!(s.columns[1], Expr::Column("name".to_string()));
            assert_eq!(s.columns[2], Expr::Column("email".to_string()));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_insert_multiple_values() {
    let stmt = parse("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')").unwrap();

    match stmt {
        Statement::Insert(s) => {
            assert_eq!(s.values.len(), 3);
            assert_eq!(s.values[0], Expr::Number(1));
            assert_eq!(s.values[1], Expr::String("Alice".to_string()));
            assert_eq!(s.values[2], Expr::String("alice@example.com".to_string()));
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_update_multiple_assignments() {
    let stmt = parse("UPDATE users SET name = 'Bob', age = 30, email = 'bob@example.com'").unwrap();

    match stmt {
        Statement::Update(s) => {
            assert_eq!(s.assignments.len(), 3);
            assert_eq!(s.assignments[0].0, "name");
            assert_eq!(s.assignments[1].0, "age");
            assert_eq!(s.assignments[2].0, "email");
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[test]
fn test_parse_with_semicolon() {
    let stmt = parse("SELECT * FROM users;").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, "users");
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_number() {
    let stmt = parse("SELECT 42").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            assert_eq!(s.columns[0], Expr::Number(42));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_string() {
    let stmt = parse("SELECT 'hello world'").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            assert_eq!(s.columns[0], Expr::String("hello world".to_string()));
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_where_equals() {
    let stmt = parse("SELECT * FROM users WHERE id = 1").unwrap();

    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
            match s.where_clause.unwrap() {
                Expr::BinaryOp { left, op: _, right } => {
                    assert_eq!(*left, Expr::Column("id".to_string()));
                    assert_eq!(*right, Expr::Number(1));
                }
                _ => panic!("Expected binary op"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_empty_string() {
    let result = parse("");
    assert!(result.is_err());
}

#[test]
fn test_parse_whitespace_only() {
    let result = parse("   \n\t  ");
    assert!(result.is_err());
}
