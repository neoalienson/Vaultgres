use rustgres::protocol::{Connection, Message, Response};
use rustgres::parser::Parser;
use std::io::Cursor;

#[test]
fn test_query_with_semicolon_integration() {
    // Test that parser handles semicolons correctly
    let sql = "SELECT 1;";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse();
    assert!(stmt.is_ok(), "Parser should handle semicolon");
}

#[test]
fn test_multiple_queries_with_semicolons() {
    let queries = vec![
        "SELECT * FROM users;",
        "INSERT INTO users VALUES (1, 'Alice');",
        "UPDATE users SET name = 'Bob' WHERE id = 1;",
        "DELETE FROM users WHERE id = 1;",
    ];
    
    for sql in queries {
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse();
        assert!(stmt.is_ok(), "Failed to parse: {}", sql);
    }
}

#[test]
fn test_error_response_format() {
    let mut buf = Vec::new();
    Response::ErrorResponse { 
        message: "Parse error: unexpected token".to_string() 
    }.write(&mut buf).unwrap();
    
    // Check message type
    assert_eq!(buf[0], b'E');
    
    // Check length is correct
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    assert_eq!(len, buf.len() - 1, "Message length mismatch");
}

#[test]
fn test_command_complete_format() {
    let mut buf = Vec::new();
    Response::CommandComplete { 
        tag: "SELECT 0".to_string() 
    }.write(&mut buf).unwrap();
    
    // Check message type
    assert_eq!(buf[0], b'C');
    
    // Check length is correct
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    assert_eq!(len, buf.len() - 1, "Message length mismatch");
}
