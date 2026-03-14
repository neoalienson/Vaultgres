#[cfg(test)]
mod tests {
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::Statement;
    use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

    #[test]
    fn test_create_table_with_serial() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Serial),
                    ColumnDef::new("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.columns[0].data_type, DataType::Serial);
    }

    #[test]
    fn test_create_table_with_boolean() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "flags".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("active".to_string(), DataType::Boolean),
                ],
            )
            .unwrap();

        let table = catalog.get_table("flags").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Boolean);
    }

    #[test]
    fn test_create_table_with_date() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "events".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("event_date".to_string(), DataType::Date),
                ],
            )
            .unwrap();

        let table = catalog.get_table("events").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Date);
    }

    #[test]
    fn test_create_table_with_time() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "schedule".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("start_time".to_string(), DataType::Time),
                ],
            )
            .unwrap();

        let table = catalog.get_table("schedule").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Time);
    }

    #[test]
    fn test_create_table_with_timestamp() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "logs".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("created_at".to_string(), DataType::Timestamp),
                ],
            )
            .unwrap();

        let table = catalog.get_table("logs").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Timestamp);
    }

    #[test]
    fn test_create_table_with_decimal() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "products".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("price".to_string(), DataType::Decimal(10, 2)),
                ],
            )
            .unwrap();

        let table = catalog.get_table("products").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Decimal(10, 2));
    }

    #[test]
    fn test_create_table_with_bytea() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "files".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("data".to_string(), DataType::Bytea),
                ],
            )
            .unwrap();

        let table = catalog.get_table("files").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Bytea);
    }

    #[test]
    fn test_parse_boolean_type() {
        let sql = "CREATE TABLE flags (id INT, active BOOLEAN)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Boolean);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_date_type() {
        let sql = "CREATE TABLE events (id INT, event_date DATE)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Date);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_time_type() {
        let sql = "CREATE TABLE schedule (id INT, start_time TIME)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Time);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_timestamp_type() {
        let sql = "CREATE TABLE logs (id INT, created_at TIMESTAMP)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Timestamp);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_decimal_type() {
        let sql = "CREATE TABLE products (id INT, price DECIMAL(10, 2))";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Decimal(10, 2));
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_bytea_type() {
        let sql = "CREATE TABLE files (id INT, data BYTEA)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Bytea);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_decimal_precision_scale() {
        let dt1 = DataType::Decimal(10, 2);
        let dt2 = DataType::Decimal(18, 4);
        assert_ne!(dt1, dt2);

        if let DataType::Decimal(p, s) = dt1 {
            assert_eq!(p, 10);
            assert_eq!(s, 2);
        }
    }

    #[test]
    fn test_boolean_default_value() {
        let catalog = Catalog::new();
        let mut col = ColumnDef::new("enabled".to_string(), DataType::Boolean);
        col.default_value = Some(Expr::Number(1));

        catalog
            .create_table(
                "settings".to_string(),
                vec![ColumnDef::new("id".to_string(), DataType::Int), col],
            )
            .unwrap();

        let table = catalog.get_table("settings").unwrap();
        assert!(table.columns[1].default_value.is_some());
    }

    #[test]
    fn test_all_data_types_in_one_table() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "all_types".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Serial),
                    ColumnDef::new("flag".to_string(), DataType::Boolean),
                    ColumnDef::new("birth_date".to_string(), DataType::Date),
                    ColumnDef::new("start_time".to_string(), DataType::Time),
                    ColumnDef::new("created_at".to_string(), DataType::Timestamp),
                    ColumnDef::new("price".to_string(), DataType::Decimal(10, 2)),
                    ColumnDef::new("data".to_string(), DataType::Bytea),
                ],
            )
            .unwrap();

        let table = catalog.get_table("all_types").unwrap();
        assert_eq!(table.columns.len(), 7);
        assert_eq!(table.columns[0].data_type, DataType::Serial);
        assert_eq!(table.columns[1].data_type, DataType::Boolean);
        assert_eq!(table.columns[2].data_type, DataType::Date);
        assert_eq!(table.columns[3].data_type, DataType::Time);
        assert_eq!(table.columns[4].data_type, DataType::Timestamp);
        assert_eq!(table.columns[5].data_type, DataType::Decimal(10, 2));
        assert_eq!(table.columns[6].data_type, DataType::Bytea);
    }
}
