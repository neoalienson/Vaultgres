use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType};
use vaultgres::parser::{Parser, Statement};

#[test]
fn test_create_and_drop_view() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    let mut parser = Parser::new("CREATE VIEW v AS SELECT * FROM users").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query).unwrap();
            assert!(catalog.get_view("v").is_some());
        }
        _ => panic!("Expected CREATE VIEW"),
    }

    catalog.drop_view("v", false).unwrap();
    assert!(catalog.get_view("v").is_none());
}

#[test]
fn test_create_view_duplicate_error() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query.clone()).unwrap();
            let result = catalog.create_view(create.name, *create.query);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("already exists"));
        }
        _ => panic!("Expected CREATE VIEW"),
    }
}

#[test]
fn test_drop_view_not_exists_error() {
    let catalog = Catalog::new();

    let result = catalog.drop_view("nonexistent", false);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_drop_view_if_exists() {
    let catalog = Catalog::new();

    let result = catalog.drop_view("nonexistent", true);
    assert!(result.is_ok());
}

#[test]
fn test_view_with_filter() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("active".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    let mut parser =
        Parser::new("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query).unwrap();
            let view = catalog.get_view("active_users").unwrap();
            assert!(view.where_clause.is_some());
        }
        _ => panic!("Expected CREATE VIEW"),
    }
}

#[test]
fn test_view_with_columns() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE VIEW user_ids AS SELECT id FROM users").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query).unwrap();
            let view = catalog.get_view("user_ids").unwrap();
            assert_eq!(view.columns.len(), 1);
        }
        _ => panic!("Expected CREATE VIEW"),
    }
}

#[test]
fn test_view_with_join() {
    let catalog = Catalog::new();

    let mut parser = Parser::new(
        "CREATE VIEW user_orders AS SELECT * FROM users INNER JOIN orders ON id = user_id",
    )
    .unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query).unwrap();
            let view = catalog.get_view("user_orders").unwrap();
            assert_eq!(view.joins.len(), 1);
        }
        _ => panic!("Expected CREATE VIEW"),
    }
}

#[test]
fn test_multiple_views() {
    let catalog = Catalog::new();

    let mut parser1 = Parser::new("CREATE VIEW v1 AS SELECT * FROM t1").unwrap();
    let stmt1 = parser1.parse().unwrap();

    let mut parser2 = Parser::new("CREATE VIEW v2 AS SELECT * FROM t2").unwrap();
    let stmt2 = parser2.parse().unwrap();

    match (stmt1, stmt2) {
        (Statement::CreateView(c1), Statement::CreateView(c2)) => {
            catalog.create_view(c1.name.clone(), *c1.query).unwrap();
            catalog.create_view(c2.name.clone(), *c2.query).unwrap();

            assert!(catalog.get_view("v1").is_some());
            assert!(catalog.get_view("v2").is_some());
        }
        _ => panic!("Expected CREATE VIEW"),
    }
}
