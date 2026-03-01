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
