use tempfile::TempDir;
use vaultgres::catalog::Catalog;
use vaultgres::parser::{Parser, Statement};

#[test]
fn test_create_and_drop_index() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE INDEX idx ON users (id)").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateIndex(create) => {
            catalog.create_index(create.clone()).unwrap();
            assert!(catalog.get_index("idx").is_some());
        }
        _ => panic!("Expected CREATE INDEX"),
    }

    catalog.drop_index("idx", false).unwrap();
    assert!(catalog.get_index("idx").is_none());
}

#[test]
fn test_create_index_duplicate_error() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE INDEX idx ON users (id)").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateIndex(create) => {
            catalog.create_index(create.clone()).unwrap();
            let result = catalog.create_index(create);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("already exists"));
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

#[test]
fn test_drop_index_not_exists_error() {
    let catalog = Catalog::new();

    let result = catalog.drop_index("nonexistent", false);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_drop_index_if_exists() {
    let catalog = Catalog::new();

    let result = catalog.drop_index("nonexistent", true);
    assert!(result.is_ok());
}

#[test]
fn test_multiple_indexes() {
    let catalog = Catalog::new();

    let mut parser1 = Parser::new("CREATE INDEX idx1 ON users (id)").unwrap();
    let stmt1 = parser1.parse().unwrap();

    let mut parser2 = Parser::new("CREATE INDEX idx2 ON orders (user_id)").unwrap();
    let stmt2 = parser2.parse().unwrap();

    match (stmt1, stmt2) {
        (Statement::CreateIndex(c1), Statement::CreateIndex(c2)) => {
            catalog.create_index(c1).unwrap();
            catalog.create_index(c2).unwrap();

            assert!(catalog.get_index("idx1").is_some());
            assert!(catalog.get_index("idx2").is_some());
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

#[test]
fn test_index_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);

        let mut parser = Parser::new("CREATE INDEX idx ON users (id)").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateIndex(create) => {
                catalog.create_index(create).unwrap();
            }
            _ => panic!("Expected CREATE INDEX"),
        }
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_index("idx").is_some());
    }
}

#[test]
fn test_unique_index() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE UNIQUE INDEX idx ON users (email)").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateIndex(create) => {
            catalog.create_index(create.clone()).unwrap();
            let index = catalog.get_index("idx").unwrap();
            assert!(index.unique);
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

#[test]
fn test_index_lifecycle() {
    let catalog = Catalog::new();

    let mut parser = Parser::new("CREATE INDEX idx ON users (id)").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateIndex(create) => {
            catalog.create_index(create).unwrap();
            assert!(catalog.get_index("idx").is_some());

            catalog.drop_index("idx", false).unwrap();
            assert!(catalog.get_index("idx").is_none());
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}
