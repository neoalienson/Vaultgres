use std::collections::HashMap;
use vaultgres::catalog::{
    Function, FunctionLanguage, FunctionRegistry, FunctionVolatility, Parameter,
};
use vaultgres::executor::{
    CursorManager, Executor, MockTupleExecutor, SetReturningFunctionExecutor,
    TableValuedFunctionExecutor,
};
use vaultgres::parser::ast::{FetchDirection, FunctionReturnType, ParameterMode, Statement};
use vaultgres::parser::Parser;

fn make_tuple(val: i64) -> HashMap<String, Vec<u8>> {
    let mut map = HashMap::new();
    map.insert("col".to_string(), val.to_string().into_bytes());
    map
}

#[test]
fn test_parse_create_function_simple() {
    let sql = "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + $2'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "add");
            assert_eq!(f.parameters.len(), 2);
            assert_eq!(f.language, "SQL");
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_create_function_with_defaults() {
    let sql = "CREATE FUNCTION greet(name TEXT, greeting TEXT = 'Hello') RETURNS TEXT LANGUAGE SQL AS 'SELECT $2 || $1'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "greet");
            assert_eq!(f.parameters.len(), 2);
            assert_eq!(f.parameters[1].default, Some("'Hello'".to_string()));
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_create_function_variadic() {
    let sql = "CREATE FUNCTION sum_all(VARIADIC nums INT) RETURNS INT LANGUAGE PLPGSQL AS 'BEGIN RETURN array_sum(nums); END'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "sum_all");
            assert_eq!(f.parameters.len(), 1);
            assert_eq!(f.parameters[0].mode, ParameterMode::Variadic);
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_create_function_out_params() {
    let sql = "CREATE FUNCTION get_stats(OUT total INT, OUT average FLOAT) RETURNS RECORD LANGUAGE SQL AS 'SELECT COUNT(*), AVG(value) FROM data'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "get_stats");
            assert_eq!(f.parameters.len(), 2);
            assert_eq!(f.parameters[0].mode, ParameterMode::Out);
            assert_eq!(f.parameters[1].mode, ParameterMode::Out);
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_create_function_returns_table() {
    let sql = "CREATE FUNCTION get_users() RETURNS TABLE(id INT, name TEXT) LANGUAGE SQL AS 'SELECT id, name FROM users'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "get_users");
            match f.return_type {
                FunctionReturnType::Table(cols) => {
                    assert_eq!(cols.len(), 2);
                    assert_eq!(cols[0].0, "id");
                    assert_eq!(cols[1].0, "name");
                }
                _ => panic!("Expected Table return type"),
            }
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_create_function_returns_setof() {
    let sql = "CREATE FUNCTION generate_series(start INT, finish INT) RETURNS SETOF INT LANGUAGE PLPGSQL AS 'BEGIN FOR i IN start..finish LOOP RETURN NEXT i; END LOOP; END'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "generate_series");
            match f.return_type {
                FunctionReturnType::Setof(type_name) => {
                    assert_eq!(type_name, "INT");
                }
                _ => panic!("Expected Setof return type"),
            }
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_drop_function() {
    let sql = "DROP FUNCTION add";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::DropFunction(f) => {
            assert_eq!(f.name, "add");
            assert!(!f.if_exists);
        }
        _ => panic!("Expected DropFunction"),
    }
}

#[test]
fn test_parse_drop_function_if_exists() {
    let sql = "DROP FUNCTION IF EXISTS add";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::DropFunction(f) => {
            assert_eq!(f.name, "add");
            assert!(f.if_exists);
        }
        _ => panic!("Expected DropFunction"),
    }
}

#[test]
fn test_parse_declare_cursor() {
    let sql = "DECLARE my_cursor CURSOR FOR SELECT * FROM users";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::DeclareCursor(c) => {
            assert_eq!(c.name, "my_cursor");
        }
        _ => panic!("Expected DeclareCursor"),
    }
}

#[test]
fn test_parse_fetch_next() {
    let sql = "FETCH NEXT FROM my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::FetchCursor(f) => {
            assert_eq!(f.name, "my_cursor");
            assert_eq!(f.direction, FetchDirection::Next);
        }
        _ => panic!("Expected FetchCursor"),
    }
}

#[test]
fn test_parse_fetch_prior() {
    let sql = "FETCH PRIOR FROM my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::FetchCursor(f) => {
            assert_eq!(f.name, "my_cursor");
            assert_eq!(f.direction, FetchDirection::Prior);
        }
        _ => panic!("Expected FetchCursor"),
    }
}

#[test]
fn test_parse_fetch_first() {
    let sql = "FETCH FIRST FROM my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::FetchCursor(f) => {
            assert_eq!(f.name, "my_cursor");
            assert_eq!(f.direction, FetchDirection::First);
        }
        _ => panic!("Expected FetchCursor"),
    }
}

#[test]
fn test_parse_fetch_last() {
    let sql = "FETCH LAST FROM my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::FetchCursor(f) => {
            assert_eq!(f.name, "my_cursor");
            assert_eq!(f.direction, FetchDirection::Last);
        }
        _ => panic!("Expected FetchCursor"),
    }
}

#[test]
fn test_parse_fetch_absolute() {
    let sql = "FETCH ABSOLUTE 5 FROM my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::FetchCursor(f) => {
            assert_eq!(f.name, "my_cursor");
            assert_eq!(f.direction, FetchDirection::Absolute);
            assert_eq!(f.count, Some(5));
        }
        _ => panic!("Expected FetchCursor"),
    }
}

#[test]
fn test_parse_fetch_relative() {
    let sql = "FETCH RELATIVE 3 FROM my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::FetchCursor(f) => {
            assert_eq!(f.name, "my_cursor");
            assert_eq!(f.direction, FetchDirection::Relative);
            assert_eq!(f.count, Some(3));
        }
        _ => panic!("Expected FetchCursor"),
    }
}

#[test]
fn test_parse_close_cursor() {
    let sql = "CLOSE my_cursor";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CloseCursor(c) => {
            assert_eq!(c.name, "my_cursor");
        }
        _ => panic!("Expected CloseCursor"),
    }
}

#[test]
fn test_cursor_manager_workflow() {
    let manager = CursorManager::new();
    let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
    let executor = Box::new(MockTupleExecutor::new(tuples));

    manager.declare("users_cursor".to_string(), executor).unwrap();

    let row1 = manager.fetch_next("users_cursor").unwrap();
    assert!(row1.is_some());

    let row2 = manager.fetch_next("users_cursor").unwrap();
    assert!(row2.is_some());

    let row_prior = manager.fetch_prior("users_cursor").unwrap();
    assert!(row_prior.is_some());

    manager.close("users_cursor").unwrap();
}

#[test]
fn test_cursor_manager_first_last() {
    let manager = CursorManager::new();
    let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
    let executor = Box::new(MockTupleExecutor::new(tuples.clone()));

    manager.declare("test_cursor".to_string(), executor).unwrap();

    let first = manager.fetch_first("test_cursor").unwrap();
    assert_eq!(first, Some(tuples[0].clone()));

    let last = manager.fetch_last("test_cursor").unwrap();
    assert_eq!(last, Some(tuples[2].clone()));
}

#[test]
fn test_cursor_manager_absolute() {
    let manager = CursorManager::new();
    let tuples = vec![make_tuple(10), make_tuple(20), make_tuple(30), make_tuple(40)];
    let executor = Box::new(MockTupleExecutor::new(tuples.clone()));

    manager.declare("test_cursor".to_string(), executor).unwrap();

    let row = manager.fetch_absolute("test_cursor", 2).unwrap();
    assert_eq!(row, Some(tuples[2].clone()));

    let row_neg = manager.fetch_absolute("test_cursor", -1).unwrap();
    assert_eq!(row_neg, Some(tuples[3].clone()));
}

#[test]
fn test_cursor_manager_relative() {
    let manager = CursorManager::new();
    let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3), make_tuple(4)];
    let executor = Box::new(MockTupleExecutor::new(tuples.clone()));

    manager.declare("test_cursor".to_string(), executor).unwrap();

    manager.fetch_next("test_cursor").unwrap();
    let row = manager.fetch_relative("test_cursor", 2).unwrap();
    assert_eq!(row, Some(tuples[3].clone()));
}

#[test]
fn test_table_valued_function_executor() {
    let func = Function {
        name: "get_range".to_string(),
        parameters: vec![],
        return_type: "TABLE".to_string(),
        language: FunctionLanguage::PlPgSql,
        body: "RETURN ARRAY[1, 2, 3]".to_string(),
        is_variadic: false,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = TableValuedFunctionExecutor::new(func, vec![]);
    let result = executor.open();
    assert!(result.is_ok());
}

#[test]
fn test_set_returning_function_executor() {
    let func = Function {
        name: "generate_nums".to_string(),
        parameters: vec![],
        return_type: "SETOF INT".to_string(),
        language: FunctionLanguage::PlPgSql,
        body: "RETURN ARRAY[1, 2, 3, 4, 5]".to_string(),
        is_variadic: false,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SetReturningFunctionExecutor::new(func, vec![]);
    let result = executor.open();
    assert!(result.is_ok());
}

#[test]
fn test_function_registry_with_table_return() {
    let mut registry = FunctionRegistry::new();
    let func = Function {
        name: "get_users".to_string(),
        parameters: vec![],
        return_type: "TABLE".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT * FROM users".to_string(),
        is_variadic: false,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    assert!(registry.register(func).is_ok());
    let resolved = registry.resolve("get_users", &[]);
    assert!(resolved.is_some());
}

#[test]
fn test_function_registry_with_setof_return() {
    let mut registry = FunctionRegistry::new();
    let func = Function {
        name: "generate_series".to_string(),
        parameters: vec![
            Parameter { name: "start".to_string(), data_type: "INT".to_string(), default: None },
            Parameter { name: "end".to_string(), data_type: "INT".to_string(), default: None },
        ],
        return_type: "SETOF INT".to_string(),
        language: FunctionLanguage::PlPgSql,
        body: "BEGIN FOR i IN start..end LOOP RETURN NEXT i; END LOOP; END".to_string(),
        is_variadic: false,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    assert!(registry.register(func).is_ok());
    let resolved = registry.resolve("generate_series", &["INT".to_string(), "INT".to_string()]);
    assert!(resolved.is_some());
}

#[test]
fn test_cursor_empty_result_set() {
    let manager = CursorManager::new();
    let tuples = vec![];
    let executor = Box::new(MockTupleExecutor::new(tuples));

    manager.declare("empty_cursor".to_string(), executor).unwrap();

    let row = manager.fetch_next("empty_cursor").unwrap();
    assert_eq!(row, None);

    let first = manager.fetch_first("empty_cursor").unwrap();
    assert_eq!(first, None);
}

#[test]
fn test_parse_function_with_immutable() {
    let sql =
        "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + $2'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "add");
            assert_eq!(f.volatility, Some(vaultgres::parser::ast::FunctionVolatility::Immutable));
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_function_with_stable() {
    let sql = "CREATE FUNCTION get_time() RETURNS TEXT LANGUAGE SQL STABLE AS 'SELECT NOW()'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "get_time");
            assert_eq!(f.volatility, Some(vaultgres::parser::ast::FunctionVolatility::Stable));
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_function_with_volatile() {
    let sql = "CREATE FUNCTION random_val() RETURNS INT LANGUAGE SQL VOLATILE AS 'SELECT RANDOM()'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "random_val");
            assert_eq!(f.volatility, Some(vaultgres::parser::ast::FunctionVolatility::Volatile));
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_parse_function_without_volatility() {
    let sql = "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + $2'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        Statement::CreateFunction(f) => {
            assert_eq!(f.name, "add");
            assert_eq!(f.volatility, None);
        }
        _ => panic!("Expected CreateFunction"),
    }
}

#[test]
fn test_function_volatility_in_registry() {
    use vaultgres::catalog::FunctionVolatility;

    let mut registry = FunctionRegistry::new();
    let func = Function {
        name: "pure_add".to_string(),
        parameters: vec![
            Parameter { name: "a".to_string(), data_type: "INT".to_string(), default: None },
            Parameter { name: "b".to_string(), data_type: "INT".to_string(), default: None },
        ],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 + $2".to_string(),
        is_variadic: false,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    registry.register(func).unwrap();
    let resolved = registry.resolve("pure_add", &["INT".to_string(), "INT".to_string()]);
    assert!(resolved.is_some());
    assert_eq!(resolved.unwrap().volatility, FunctionVolatility::Immutable);
}
