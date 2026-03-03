use vaultgres::catalog::Catalog;
use vaultgres::protocol::{Connection, Message, Response, Server};
use std::io::Cursor;
use std::sync::Arc;

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
    let catalog = Arc::new(Catalog::new());
    let _conn = Connection::new(stream, catalog);
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

#[test]
fn test_server_bind_with_data_dir() {
    use tempfile::TempDir;
    let temp = TempDir::new().unwrap();
    let server =
        Server::bind_with_data_dir("127.0.0.1:0", temp.path().to_str().unwrap().to_string())
            .unwrap();
    assert!(server.local_addr().is_ok());
}

#[test]
fn test_connection_handle_query_create_table() {
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("CREATE TABLE t (id INT)");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_invalid_sql() {
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("INVALID SQL");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_drop_table() {
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    catalog.create_table("t".to_string(), vec![]).unwrap();
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("DROP TABLE t");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_insert() {
    use vaultgres::parser::ast::{ColumnDef, DataType};
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("t".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("INSERT INTO t VALUES (1)");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_select() {
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    catalog.create_table("t".to_string(), vec![]).unwrap();
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("SELECT * FROM t");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_update() {
    use vaultgres::parser::ast::{ColumnDef, DataType};
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("t".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("UPDATE t SET id = 1");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_delete() {
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    catalog.create_table("t".to_string(), vec![]).unwrap();
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("DELETE FROM t");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_describe() {
    use vaultgres::parser::ast::{ColumnDef, DataType};
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("t".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("DESCRIBE t");
    assert!(result.is_ok());
}

#[test]
fn test_connection_handle_query_describe_nonexistent() {
    let stream = Cursor::new(Vec::new());
    let catalog = Arc::new(Catalog::new());
    let mut conn = Connection::new(stream, catalog);
    let result = conn.handle_query("DESCRIBE nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_server_shutdown() {
    use tempfile::TempDir;
    let temp = TempDir::new().unwrap();
    let server =
        Server::bind_with_data_dir("127.0.0.1:0", temp.path().to_str().unwrap().to_string())
            .unwrap();
    let result = server.shutdown();
    assert!(result.is_ok());
}
