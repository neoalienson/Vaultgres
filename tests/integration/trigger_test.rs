use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{TriggerEvent, TriggerTiming};
use vaultgres::parser::{Parser, Statement};

#[test]
fn test_create_and_drop_trigger() {
    let catalog = Catalog::new();

    let mut parser =
        Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create.clone()).unwrap();
            assert!(catalog.get_trigger("t").is_some());
        }
        _ => panic!("Expected CREATE TRIGGER"),
    }

    catalog.drop_trigger("t", false).unwrap();
    assert!(catalog.get_trigger("t").is_none());
}

#[test]
fn test_create_trigger_duplicate_error() {
    let catalog = Catalog::new();

    let mut parser =
        Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create.clone()).unwrap();
            let result = catalog.create_trigger(create);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("already exists"));
        }
        _ => panic!("Expected CREATE TRIGGER"),
    }
}

#[test]
fn test_drop_trigger_not_exists_error() {
    let catalog = Catalog::new();

    let result = catalog.drop_trigger("nonexistent", false);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_drop_trigger_if_exists() {
    let catalog = Catalog::new();

    let result = catalog.drop_trigger("nonexistent", true);
    assert!(result.is_ok());
}

#[test]
fn test_multiple_triggers() {
    let catalog = Catalog::new();

    let mut parser1 =
        Parser::new("CREATE TRIGGER t1 BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
    let stmt1 = parser1.parse().unwrap();

    let mut parser2 =
        Parser::new("CREATE TRIGGER t2 AFTER UPDATE ON orders FOR EACH ROW BEGIN END").unwrap();
    let stmt2 = parser2.parse().unwrap();

    match (stmt1, stmt2) {
        (Statement::CreateTrigger(c1), Statement::CreateTrigger(c2)) => {
            catalog.create_trigger(c1).unwrap();
            catalog.create_trigger(c2).unwrap();

            assert!(catalog.get_trigger("t1").is_some());
            assert!(catalog.get_trigger("t2").is_some());
        }
        _ => panic!("Expected CREATE TRIGGER"),
    }
}

#[test]
fn test_trigger_timing_preserved() {
    let catalog = Catalog::new();

    let mut parser =
        Parser::new("CREATE TRIGGER t AFTER DELETE ON users FOR EACH ROW BEGIN END").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create).unwrap();
            let trigger = catalog.get_trigger("t").unwrap();
            assert_eq!(trigger.timing, TriggerTiming::After);
            assert_eq!(trigger.event, TriggerEvent::Delete);
        }
        _ => panic!("Expected CREATE TRIGGER"),
    }
}

#[test]
fn test_trigger_with_when_clause() {
    let catalog = Catalog::new();

    let mut parser = Parser::new(
        "CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW WHEN (active = 1) BEGIN END",
    )
    .unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create).unwrap();
            let trigger = catalog.get_trigger("t").unwrap();
            assert!(trigger.when.is_some());
        }
        _ => panic!("Expected CREATE TRIGGER"),
    }
}

#[test]
fn test_trigger_lifecycle() {
    let catalog = Catalog::new();

    let mut parser =
        Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create).unwrap();
            assert!(catalog.get_trigger("t").is_some());

            catalog.drop_trigger("t", false).unwrap();
            assert!(catalog.get_trigger("t").is_none());
        }
        _ => panic!("Expected CREATE TRIGGER"),
    }
}
