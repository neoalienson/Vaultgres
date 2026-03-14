#[cfg(test)]
mod tests {
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::{Expr, Statement};

    fn parse_select(sql: &str) -> vaultgres::parser::ast::SelectStmt {
        let mut parser = Parser::new(sql).unwrap();
        match parser.parse().unwrap() {
            Statement::Select(s) => s,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_table_alias() {
        let s = parse_select("SELECT * FROM users AS u");
        assert_eq!(s.from, "users");
        assert_eq!(s.table_alias, Some("u".to_string()));
    }

    #[test]
    fn test_parse_table_no_alias() {
        let s = parse_select("SELECT * FROM users");
        assert_eq!(s.from, "users");
        assert_eq!(s.table_alias, None);
    }

    #[test]
    fn test_parse_column_alias() {
        let s = parse_select("SELECT id AS user_id FROM users");
        assert_eq!(s.columns.len(), 1);
        match &s.columns[0] {
            Expr::Alias { alias, .. } => assert_eq!(alias, "user_id"),
            _ => panic!("Expected Alias expression"),
        }
    }

    #[test]
    fn test_parse_multiple_column_aliases() {
        let s = parse_select("SELECT id AS user_id, name AS user_name FROM users");
        assert_eq!(s.columns.len(), 2);
        match &s.columns[0] {
            Expr::Alias { alias, .. } => assert_eq!(alias, "user_id"),
            _ => panic!("Expected Alias expression"),
        }
        match &s.columns[1] {
            Expr::Alias { alias, .. } => assert_eq!(alias, "user_name"),
            _ => panic!("Expected Alias expression"),
        }
    }

    #[test]
    fn test_parse_join_table_alias() {
        let s = parse_select("SELECT * FROM users AS u JOIN orders AS o ON id = id");
        assert_eq!(s.table_alias, Some("u".to_string()));
        assert_eq!(s.joins.len(), 1);
        assert_eq!(s.joins[0].alias, Some("o".to_string()));
    }

    #[test]
    fn test_parse_mixed_aliases() {
        let s = parse_select("SELECT id AS user_id, name FROM users AS u");
        assert_eq!(s.table_alias, Some("u".to_string()));
        assert_eq!(s.columns.len(), 2);
        match &s.columns[0] {
            Expr::Alias { alias, .. } => assert_eq!(alias, "user_id"),
            _ => panic!("Expected Alias expression"),
        }
        match &s.columns[1] {
            Expr::Column(name) => assert_eq!(name, "name"),
            _ => panic!("Expected Column expression"),
        }
    }

    #[test]
    fn test_parse_aggregate_with_alias() {
        let s = parse_select("SELECT COUNT(id) AS total FROM users");
        assert_eq!(s.columns.len(), 1);
        match &s.columns[0] {
            Expr::Alias { alias, expr } => {
                assert_eq!(alias, "total");
                assert!(matches!(**expr, Expr::Aggregate { .. }));
            }
            _ => panic!("Expected Alias expression"),
        }
    }

    #[test]
    fn test_parse_expression_with_alias() {
        let s = parse_select("SELECT 1 AS one FROM users");
        assert_eq!(s.columns.len(), 1);
        match &s.columns[0] {
            Expr::Alias { alias, expr } => {
                assert_eq!(alias, "one");
                assert!(matches!(**expr, Expr::Number(1)));
            }
            _ => panic!("Expected Alias expression"),
        }
    }

    #[test]
    fn test_parse_table_and_join_aliases() {
        let s = parse_select("SELECT * FROM users AS u LEFT JOIN orders AS o ON id = id");
        assert_eq!(s.from, "users");
        assert_eq!(s.table_alias, Some("u".to_string()));
        assert_eq!(s.joins.len(), 1);
        assert_eq!(s.joins[0].table, "orders");
        assert_eq!(s.joins[0].alias, Some("o".to_string()));
    }

    #[test]
    fn test_parse_no_aliases() {
        let s = parse_select("SELECT id, name FROM users");
        assert_eq!(s.table_alias, None);
        assert_eq!(s.columns.len(), 2);
        match &s.columns[0] {
            Expr::Column(name) => assert_eq!(name, "id"),
            _ => panic!("Expected Column expression"),
        }
        match &s.columns[1] {
            Expr::Column(name) => assert_eq!(name, "name"),
            _ => panic!("Expected Column expression"),
        }
    }
}
