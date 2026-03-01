//! Edge case tests for protocol layer

#[cfg(test)]
mod tests {
    use crate::protocol::message::*;

    #[test]
    fn test_parse_empty_query() {
        let msg = Message::parse(b'Q', b"\0").unwrap();
        assert_eq!(msg, Message::Query { sql: "".to_string() });
    }

    #[test]
    fn test_parse_query_no_null_terminator() {
        let msg = Message::parse(b'Q', b"SELECT 1").unwrap();
        assert_eq!(msg, Message::Query { sql: "SELECT 1".to_string() });
    }

    #[test]
    fn test_parse_unknown_message_type() {
        let result = Message::parse(b'Z', b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_startup_empty_data() {
        let msg = Message::parse(0, b"\0").unwrap();
        assert_eq!(msg, Message::Startup { user: "".to_string(), database: "".to_string() });
    }

    #[test]
    fn test_startup_partial_data() {
        let data = b"user=test\0";
        let msg = Message::parse(0, data).unwrap();
        match msg {
            Message::Startup { user, .. } => assert_eq!(user, "test"),
            _ => panic!("Expected Startup message"),
        }
    }

    #[test]
    fn test_response_command_complete_empty_tag() {
        let mut buf = Vec::new();
        Response::CommandComplete { tag: "".to_string() }.write(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_response_error_empty_message() {
        let mut buf = Vec::new();
        Response::ErrorResponse { message: "".to_string() }.write(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_response_error_long_message() {
        let mut buf = Vec::new();
        let long_msg = "x".repeat(1000);
        Response::ErrorResponse { message: long_msg }.write(&mut buf).unwrap();
        assert!(buf.len() > 1000);
    }

    #[test]
    fn test_query_with_special_chars() {
        let data = b"SELECT * FROM \"table\"\0";
        let msg = Message::parse(b'Q', data).unwrap();
        assert_eq!(msg, Message::Query { sql: "SELECT * FROM \"table\"".to_string() });
    }

    #[test]
    fn test_multiple_null_terminators() {
        let msg = Message::parse(b'Q', b"SELECT 1\0\0\0").unwrap();
        assert_eq!(msg, Message::Query { sql: "SELECT 1".to_string() });
    }

    #[test]
    fn test_startup_with_database_only() {
        let data = b"database=mydb\0";
        let msg = Message::parse(0, data).unwrap();
        match msg {
            Message::Startup { database, .. } => assert_eq!(database, "mydb"),
            _ => panic!("Expected Startup"),
        }
    }

    #[test]
    fn test_response_row_description() {
        let mut buf = Vec::new();
        Response::RowDescription { columns: vec!["id".to_string()] }.write(&mut buf).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn test_response_data_row() {
        let mut buf = Vec::new();
        Response::DataRow { values: vec![vec![1, 2, 3]] }.write(&mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
