use vaultgres::catalog::Catalog;
use vaultgres::parser::{Parser, Statement};

#[test]
fn test_create_and_drop_materialized_view() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateMaterializedView(create) => {
            catalog.create_materialized_view(create.name.clone(), *create.query).unwrap();
            assert!(catalog.get_materialized_view("mv").is_some());
        }
        _ => panic!("Expected CREATE MATERIALIZED VIEW"),
    }

    catalog.drop_materialized_view("mv", false).unwrap();
    assert!(catalog.get_materialized_view("mv").is_none());
}

#[test]
fn test_create_materialized_view_duplicate_error() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateMaterializedView(create) => {
            catalog.create_materialized_view(create.name.clone(), *create.query.clone()).unwrap();
            let result = catalog.create_materialized_view(create.name, *create.query);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("already exists"));
        }
        _ => panic!("Expected CREATE MATERIALIZED VIEW"),
    }
}

#[test]
fn test_refresh_materialized_view() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateMaterializedView(create) => {
            catalog.create_materialized_view(create.name.clone(), *create.query).unwrap();
            let result = catalog.refresh_materialized_view("mv");
            assert!(result.is_ok());
        }
        _ => panic!("Expected CREATE MATERIALIZED VIEW"),
    }
}

#[test]
fn test_refresh_nonexistent_materialized_view() {
    let catalog = Catalog::new();

    let result = catalog.refresh_materialized_view("nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_drop_materialized_view_not_exists_error() {
    let catalog = Catalog::new();

    let result = catalog.drop_materialized_view("nonexistent", false);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_drop_materialized_view_if_exists() {
    let catalog = Catalog::new();

    let result = catalog.drop_materialized_view("nonexistent", true);
    assert!(result.is_ok());
}

#[test]
fn test_multiple_materialized_views() {
    let catalog = Catalog::new();

    let mut parser1 = Parser::new("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1").unwrap();
    let stmt1 = parser1.parse().unwrap();

    let mut parser2 = Parser::new("CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM t2").unwrap();
    let stmt2 = parser2.parse().unwrap();

    match (stmt1, stmt2) {
        (Statement::CreateMaterializedView(c1), Statement::CreateMaterializedView(c2)) => {
            catalog.create_materialized_view(c1.name.clone(), *c1.query).unwrap();
            catalog.create_materialized_view(c2.name.clone(), *c2.query).unwrap();

            assert!(catalog.get_materialized_view("mv1").is_some());
            assert!(catalog.get_materialized_view("mv2").is_some());
        }
        _ => panic!("Expected CREATE MATERIALIZED VIEW"),
    }
}

#[test]
fn test_materialized_view_lifecycle() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateMaterializedView(create) => {
            catalog.create_materialized_view(create.name.clone(), *create.query).unwrap();
            assert!(catalog.get_materialized_view("mv").is_some());

            catalog.refresh_materialized_view("mv").unwrap();
            assert!(catalog.get_materialized_view("mv").is_some());

            catalog.drop_materialized_view("mv", false).unwrap();
            assert!(catalog.get_materialized_view("mv").is_none());
        }
        _ => panic!("Expected CREATE MATERIALIZED VIEW"),
    }
}
