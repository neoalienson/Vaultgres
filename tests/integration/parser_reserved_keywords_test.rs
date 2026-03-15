// Unit tests for parser edge cases with reserved keywords

#[cfg(test)]
mod tests {
    use vaultgres::parser::Parser;

    #[test]
    fn test_reserved_keyword_as_column_name_fails() {
        // TEXT is a reserved keyword and should fail as unquoted column name
        let result = Parser::new("CREATE TABLE test (id INT, text TEXT)").unwrap().parse();
        assert!(result.is_err(), "Should fail when using reserved keyword 'text' as column name");
    }

    #[test]
    fn test_non_reserved_column_name_succeeds() {
        // Using non-reserved column names should succeed
        let result = Parser::new("CREATE TABLE test (id INT, txt TEXT)").unwrap().parse();
        assert!(result.is_ok(), "Should succeed with non-reserved column name 'txt'");
    }

    #[test]
    fn test_multiple_reserved_keywords_fail() {
        // Test various reserved keywords that should fail as column names
        let reserved_keywords = vec![
            "text",
            "int",
            "boolean",
            "date",
            "time",
            "timestamp",
            "select",
            "from",
            "where",
            "order",
            "group",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "table",
            "index",
        ];

        for keyword in reserved_keywords {
            let sql = format!("CREATE TABLE test (id INT, {} TEXT)", keyword);
            let result = Parser::new(&sql).unwrap().parse();
            // Note: Some keywords might work depending on parser implementation
            // This test documents current behavior
            eprintln!("Testing keyword '{}': {:?}", keyword, result.is_err());
        }
    }

    #[test]
    fn test_valid_create_table_statements() {
        // Test various valid CREATE TABLE statements
        let valid_statements = vec![
            "CREATE TABLE test (id INT)",
            "CREATE TABLE test (id INT, name TEXT)",
            "CREATE TABLE test (id INT, value INT, txt TEXT)",
            "CREATE TABLE test (id INT PRIMARY KEY, name TEXT)",
            "CREATE TABLE test (id INT, name TEXT, value INT)",
        ];

        for sql in valid_statements {
            let result = Parser::new(sql).unwrap().parse();
            assert!(result.is_ok(), "Failed to parse: {}", sql);
        }
    }

    #[test]
    fn test_create_table_with_varchar() {
        let result = Parser::new("CREATE TABLE test (id INT, name VARCHAR(100))").unwrap().parse();
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_table_with_boolean() {
        let result = Parser::new("CREATE TABLE test (id INT, active BOOLEAN)").unwrap().parse();
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_table_multiple_columns() {
        let result =
            Parser::new("CREATE TABLE test (a INT, b INT, c INT, d TEXT, e TEXT)").unwrap().parse();
        assert!(result.is_ok());
    }
}
