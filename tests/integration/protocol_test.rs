use rustgres::protocol::{Message, Response, Connection, Server};
use std::io::Cursor;

#[test]
fn test_message_parsing() {
    let query = Message::parse(b'Q', b"SELECT * FROM users\0").unwrap();
    assert_eq!(query, Message::Query { sql: "SELECT * FROM users".to_string() });

    let term = Message::parse(b'X', &[]).unwrap();
    assert_eq!(term, Message::Terminate);
}

#[test]
fn test_response_serialization() {
    let mut buf = Vec::new();
    Response::AuthenticationOk.write(&mut buf).unwrap();
    assert!(!buf.is_empty());
    assert_eq!(&buf[0..1], b"R");

    let mut buf = Vec::new();
    Response::ReadyForQuery.write(&mut buf).unwrap();
    assert_eq!(&buf[0..1], b"Z");

    let mut buf = Vec::new();
    Response::CommandComplete { tag: "SELECT 1".to_string() }.write(&mut buf).unwrap();
    assert_eq!(&buf[0..1], b"C");
}

#[test]
fn test_connection_creation() {
    let stream = Cursor::new(Vec::new());
    let _conn = Connection::new(stream);
}

#[test]
fn test_server_bind() {
    let server = Server::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    assert!(addr.port() > 0);
}

#[test]
fn test_startup_message_parsing() {
    let data = b"user=postgres\0database=testdb\0\0";
    let msg = Message::parse(0, data).unwrap();
    match msg {
        Message::Startup { user, database } => {
            assert_eq!(user, "postgres");
            assert_eq!(database, "testdb");
        }
        _ => panic!("Expected Startup message"),
    }
}

#[test]
fn test_error_response() {
    let mut buf = Vec::new();
    Response::ErrorResponse { message: "Test error".to_string() }.write(&mut buf).unwrap();
    assert_eq!(&buf[0..1], b"E");
}

#[test]
fn test_query_with_semicolon() {
    let data = b"SELECT 1;\0";
    let msg = Message::parse(b'Q', data).unwrap();
    assert_eq!(msg, Message::Query { sql: "SELECT 1;".to_string() });
}

#[test]
fn test_error_response_length() {
    let mut buf = Vec::new();
    Response::ErrorResponse { message: "Parse error".to_string() }.write(&mut buf).unwrap();
    
    // Verify message type
    assert_eq!(buf[0], b'E');
    
    // Verify length field exists
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    assert!(len > 0);
    assert_eq!(len as usize, buf.len() - 1);
}
