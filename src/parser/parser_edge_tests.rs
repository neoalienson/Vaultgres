use super::*;

// Empty/Whitespace Tests
#[test]
fn test_parse_empty_string() {
    let result = parse("");
    assert!(result.is_err());
}

#[test]
fn test_parse_whitespace_only() {
    let result = parse("   \t\n  ");
    assert!(result.is_err());
}

#[test]
fn test_parse_only_semicolon() {
    let result = parse(";");
    assert!(result.is_err());
}

#[test]
fn test_parse_multiple_semicolons() {
    let result = parse(";;;");
    assert!(result.is_err());
}

// SELECT Edge Cases
#[test]
fn test_parse_incomplete_select() {
    let result = parse("SELECT");
    assert!(result.is_err());
}

#[test]
fn test_parse_select_invalid_column_list() {
    let result = parse("SELECT , FROM users");
    assert!(result.is_err());
}

#[test]
fn test_parse_select_trailing_comma() {
    let result = parse("SELECT id, name, FROM users");
    assert!(result.is_err());
}

#[test]
fn test_parse_select_no_from() {
    let result = parse("SELECT 1");
    assert!(result.is_ok());
}

#[test]
fn test_parse_select_star_with_columns() {
    let result = parse("SELECT *, id FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_parse_missing_table_name() {
    let result = parse("SELECT * FROM");
    assert!(result.is_err());
}

#[test]
fn test_parse_select_from_number() {
    let result = parse("SELECT * FROM 123");
    assert!(result.is_err());
}

// WHERE Clause Edge Cases
#[test]
fn test_parse_invalid_where_clause() {
    let result = parse("SELECT * FROM users WHERE");
    assert!(result.is_err());
}

#[test]
fn test_parse_where_incomplete_comparison() {
    let result = parse("SELECT * FROM users WHERE id =");
    assert!(result.is_err());
}

#[test]
fn test_parse_where_missing_operator() {
    let result = parse("SELECT * FROM users WHERE id 5");
    assert!(result.is_ok()); // Parser treats "5" as separate statement
}

#[test]
fn test_parse_where_double_and() {
    let result = parse("SELECT * FROM users WHERE id = 1 AND AND name = 'x'");
    assert!(result.is_err());
}

#[test]
fn test_parse_where_trailing_and() {
    let result = parse("SELECT * FROM users WHERE id = 1 AND");
    assert!(result.is_err());
}

#[test]
fn test_parse_where_trailing_or() {
    let result = parse("SELECT * FROM users WHERE id = 1 OR");
    assert!(result.is_err());
}

// INSERT Edge Cases
#[test]
fn test_parse_insert_without_values() {
    let result = parse("INSERT INTO users");
    assert!(result.is_err());
}

#[test]
fn test_parse_insert_missing_into() {
    let result = parse("INSERT users VALUES (1)");
    assert!(result.is_err());
}

#[test]
fn test_parse_insert_empty_values() {
    let result = parse("INSERT INTO users VALUES ()");
    assert!(result.is_err());
}

#[test]
fn test_parse_insert_unclosed_paren() {
    let result = parse("INSERT INTO users VALUES (1, 2");
    assert!(result.is_err());
}

#[test]
fn test_parse_insert_extra_comma() {
    let result = parse("INSERT INTO users VALUES (1,, 2)");
    assert!(result.is_err());
}

// UPDATE Edge Cases
#[test]
fn test_parse_update_without_set() {
    let result = parse("UPDATE users");
    assert!(result.is_err());
}

#[test]
fn test_parse_update_missing_assignment() {
    let result = parse("UPDATE users SET");
    assert!(result.is_err());
}

#[test]
fn test_parse_update_incomplete_assignment() {
    let result = parse("UPDATE users SET name =");
    assert!(result.is_err());
}

#[test]
fn test_parse_update_missing_equals() {
    let result = parse("UPDATE users SET name 'Bob'");
    assert!(result.is_err());
}

#[test]
fn test_parse_update_trailing_comma() {
    let result = parse("UPDATE users SET name = 'Bob',");
    assert!(result.is_err());
}

// DELETE Edge Cases
#[test]
fn test_parse_delete_without_from() {
    let result = parse("DELETE users");
    assert!(result.is_err());
}

#[test]
fn test_parse_delete_missing_table() {
    let result = parse("DELETE FROM");
    assert!(result.is_err());
}

#[test]
fn test_parse_delete_all_ok() {
    let result = parse("DELETE FROM users");
    assert!(result.is_ok());
}

// CREATE TABLE Edge Cases
#[test]
fn test_parse_create_table_without_columns() {
    let result = parse("CREATE TABLE users");
    assert!(result.is_err());
}

#[test]
fn test_parse_create_table_empty_columns() {
    let result = parse("CREATE TABLE users ()");
    assert!(result.is_err());
}

#[test]
fn test_parse_create_table_missing_type() {
    let result = parse("CREATE TABLE users (id)");
    assert!(result.is_err());
}

#[test]
fn test_parse_create_table_invalid_type() {
    let result = parse("CREATE TABLE users (id INVALID)");
    assert!(result.is_err());
}

#[test]
fn test_parse_create_table_trailing_comma() {
    let result = parse("CREATE TABLE users (id INT,)");
    assert!(result.is_err());
}

#[test]
fn test_parse_create_table_unclosed_paren() {
    let result = parse("CREATE TABLE users (id INT");
    assert!(result.is_err());
}

// DROP TABLE Edge Cases
#[test]
fn test_parse_drop_table_nonexistent_ok() {
    let result = parse("DROP TABLE nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_parse_drop_table_missing_name() {
    let result = parse("DROP TABLE");
    assert!(result.is_err());
}

#[test]
fn test_parse_drop_table_if_without_exists() {
    let result = parse("DROP TABLE IF users");
    assert!(result.is_err());
}

// String Literal Edge Cases
#[test]
fn test_parse_unclosed_string() {
    let result = parse("SELECT * FROM users WHERE name = 'unclosed");
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_string_literal() {
    let result = parse("SELECT * FROM users WHERE name = ''");
    assert!(result.is_ok());
}

#[test]
fn test_parse_string_with_spaces() {
    let result = parse("SELECT * FROM users WHERE name = '  spaces  '");
    assert!(result.is_ok());
}

// Number Edge Cases
#[test]
fn test_parse_zero() {
    let result = parse("SELECT * FROM users WHERE id = 0");
    assert!(result.is_ok());
}

#[test]
fn test_parse_large_number() {
    let result = parse("SELECT * FROM users WHERE id = 999999999");
    assert!(result.is_ok());
}

#[test]
fn test_parse_negative_number() {
    let result = parse("SELECT * FROM users WHERE id = -1");
    assert!(result.is_err()); // Negative numbers not supported in lexer
}

// Operator Edge Cases
#[test]
fn test_parse_invalid_operator() {
    let result = parse("SELECT * FROM users WHERE id @ 5");
    assert!(result.is_err());
}

#[test]
fn test_parse_incomplete_not_equals() {
    let result = parse("SELECT * FROM users WHERE id ! 5");
    assert!(result.is_err());
}

// Identifier Edge Cases
#[test]
fn test_parse_reserved_word_as_table() {
    let result = parse("SELECT * FROM select");
    assert!(result.is_err());
}

#[test]
fn test_parse_underscore_identifier() {
    let result = parse("SELECT * FROM user_data");
    assert!(result.is_ok());
}

#[test]
fn test_parse_number_in_identifier() {
    let result = parse("SELECT * FROM users2");
    assert!(result.is_ok());
}

// Syntax Edge Cases
#[test]
fn test_parse_invalid_syntax() {
    let result = parse("INVALID SQL STATEMENT");
    assert!(result.is_err());
}

#[test]
fn test_parse_missing_semicolon_ok() {
    let result = parse("SELECT * FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_parse_extra_commas() {
    let result = parse("SELECT id,, name FROM users");
    assert!(result.is_err());
}

#[test]
fn test_parse_unmatched_parens() {
    let result = parse("SELECT * FROM users WHERE (id = 1");
    assert!(result.is_err());
}

#[test]
fn test_parse_extra_closing_paren() {
    let result = parse("SELECT * FROM users WHERE id = 1)");
    assert!(result.is_ok());
}

// LIMIT/OFFSET Edge Cases
#[test]
fn test_parse_limit_without_number() {
    let result = parse("SELECT * FROM users LIMIT");
    assert!(result.is_err());
}

#[test]
fn test_parse_offset_without_number() {
    let result = parse("SELECT * FROM users OFFSET");
    assert!(result.is_err());
}

#[test]
fn test_parse_limit_zero() {
    let result = parse("SELECT * FROM users LIMIT 0");
    assert!(result.is_ok());
}

#[test]
fn test_parse_offset_zero() {
    let result = parse("SELECT * FROM users OFFSET 0");
    assert!(result.is_ok());
}

// ORDER BY Edge Cases
#[test]
fn test_parse_order_by_without_column() {
    let result = parse("SELECT * FROM users ORDER BY");
    assert!(result.is_err());
}

#[test]
fn test_parse_order_by_missing_by() {
    let result = parse("SELECT * FROM users ORDER id");
    assert!(result.is_err());
}

// Aggregate Edge Cases
#[test]
fn test_parse_count_without_parens() {
    let result = parse("SELECT COUNT FROM users");
    assert!(result.is_err());
}

#[test]
fn test_parse_count_empty_parens() {
    let result = parse("SELECT COUNT() FROM users");
    assert!(result.is_err());
}

// Case Sensitivity
#[test]
fn test_parse_lowercase_keywords() {
    let result = parse("select * from users");
    assert!(result.is_ok());
}

#[test]
fn test_parse_mixed_case_keywords() {
    let result = parse("SeLeCt * FrOm users");
    assert!(result.is_ok());
}

// PRIMARY KEY and FOREIGN KEY tests
#[test]
fn test_parse_primary_key_column_level() {
    let mut parser = Parser::new("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "users");
            assert_eq!(s.columns.len(), 2);
            assert!(s.columns[0].is_primary_key);
            assert!(!s.columns[1].is_primary_key);
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_primary_key_table_level() {
    let mut parser =
        Parser::new("CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "users");
            assert_eq!(s.primary_key, Some(vec!["id".to_string()]));
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_composite_primary_key() {
    let mut parser = Parser::new(
        "CREATE TABLE orders (user_id INT, product_id INT, PRIMARY KEY (user_id, product_id))",
    )
    .unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "orders");
            assert_eq!(s.primary_key, Some(vec!["user_id".to_string(), "product_id".to_string()]));
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_foreign_key_column_level() {
    let mut parser =
        Parser::new("CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id))")
            .unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "orders");
            assert_eq!(s.columns.len(), 2);
            assert!(s.columns[1].foreign_key.is_some());
            let fk = s.columns[1].foreign_key.as_ref().unwrap();
            assert_eq!(fk.table, "customers");
            assert_eq!(fk.column, "id");
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_foreign_key_table_level() {
    let mut parser = Parser::new("CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customers(id))").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "orders");
            assert_eq!(s.foreign_keys.len(), 1);
            assert_eq!(s.foreign_keys[0].columns, vec!["customer_id".to_string()]);
            assert_eq!(s.foreign_keys[0].ref_table, "customers");
            assert_eq!(s.foreign_keys[0].ref_columns, vec!["id".to_string()]);
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_pk_and_fk_combined() {
    let mut parser = Parser::new(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))",
    )
    .unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "orders");
            assert!(s.columns[0].is_primary_key);
            assert!(s.columns[1].foreign_key.is_some());
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_multiple_foreign_keys() {
    let mut parser = Parser::new("CREATE TABLE order_items (order_id INT REFERENCES orders(id), product_id INT REFERENCES products(id))").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTable(s) => {
            assert_eq!(s.table, "order_items");
            assert!(s.columns[0].foreign_key.is_some());
            assert!(s.columns[1].foreign_key.is_some());
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

// NULL Expression Tests
#[test]
fn test_parse_null_literal() {
    let result = parse("SELECT NULL");
    assert!(result.is_ok());
    if let Statement::Select(stmt) = result.unwrap() {
        assert!(matches!(stmt.columns[0], Expr::Null));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_null_in_where_clause() {
    let result = parse("SELECT * FROM users WHERE value IS NULL");
    assert!(result.is_ok());
    if let Statement::Select(stmt) = result.unwrap() {
        assert!(matches!(stmt.where_clause, Some(Expr::IsNull(_))));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_is_not_null() {
    let result = parse("SELECT * FROM users WHERE value IS NOT NULL");
    assert!(result.is_ok());
    if let Statement::Select(stmt) = result.unwrap() {
        assert!(matches!(stmt.where_clause, Some(Expr::IsNotNull(_))));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_null_in_insert() {
    let result = parse("INSERT INTO users VALUES (1, NULL, 'test')");
    assert!(result.is_ok());
    if let Statement::Insert(stmt) = result.unwrap() {
        assert!(matches!(stmt.values[1], Expr::Null));
    } else {
        panic!("Expected INSERT statement");
    }
}

#[test]
fn test_parse_null_in_update() {
    let result = parse("UPDATE users SET value = NULL WHERE id = 1");
    assert!(result.is_ok());
    if let Statement::Update(stmt) = result.unwrap() {
        assert!(matches!(stmt.assignments[0].1, Expr::Null));
    } else {
        panic!("Expected UPDATE statement");
    }
}

#[test]
fn test_parse_null_in_case() {
    let result = parse("SELECT CASE WHEN value IS NULL THEN 'missing' ELSE value END FROM users");
    assert!(result.is_ok());
    if let Statement::Select(stmt) = result.unwrap() {
        assert!(matches!(stmt.columns[0], Expr::Case { .. }));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_null_in_coalesce() {
    let result = parse("SELECT COALESCE(NULL, NULL, 'default')");
    assert!(result.is_ok());
    if let Statement::Select(stmt) = result.unwrap() {
        assert!(matches!(&stmt.columns[0], Expr::FunctionCall { name, .. } if name == "COALESCE"));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_null_comparison() {
    let result = parse("SELECT * FROM users WHERE value = NULL");
    assert!(result.is_ok());
    if let Statement::Select(stmt) = result.unwrap() {
        assert!(matches!(
            stmt.where_clause,
            Some(Expr::BinaryOp { right, .. }) if matches!(*right, Expr::Null)
        ));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_multiple_nulls() {
    let result = parse("INSERT INTO users VALUES (NULL, NULL, NULL)");
    assert!(result.is_ok());
    if let Statement::Insert(stmt) = result.unwrap() {
        for expr in &stmt.values {
            assert!(matches!(expr, Expr::Null));
        }
    } else {
        panic!("Expected INSERT statement");
    }
}

#[test]
fn test_parse_null_with_other_values() {
    let result = parse("INSERT INTO users VALUES (1, NULL, 'test', NULL, 42)");
    assert!(result.is_ok());
    if let Statement::Insert(stmt) = result.unwrap() {
        assert!(matches!(stmt.values[0], Expr::Number(1)));
        assert!(matches!(stmt.values[1], Expr::Null));
        assert!(matches!(stmt.values[2], Expr::String(_)));
        assert!(matches!(stmt.values[3], Expr::Null));
        assert!(matches!(stmt.values[4], Expr::Number(42)));
    } else {
        panic!("Expected INSERT statement");
    }
}
