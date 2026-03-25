use std::sync::Arc;
use vaultgres::catalog::{Catalog, Function, FunctionLanguage};
use vaultgres::executor::{Executor, volcano::SqlFunctionExecutor};
use vaultgres::parser::Parser;

#[test]
fn test_sql_function_scalar_add() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "add".to_string(),
        parameters: vec![],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 + $2".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(
        func,
        vec![vaultgres::catalog::Value::Int(5), vaultgres::catalog::Value::Int(3)],
        catalog,
    );

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Int(8));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_sql_function_scalar_text_concat() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "greet".to_string(),
        parameters: vec![],
        return_type: "TEXT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT CONCAT('Hello, ', $1)".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(
        func,
        vec![vaultgres::catalog::Value::Text("World".to_string())],
        catalog,
    );

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Text("Hello, World".to_string()));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_sql_function_with_null_param() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "add".to_string(),
        parameters: vec![],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 + $2".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(
        func,
        vec![vaultgres::catalog::Value::Int(5), vaultgres::catalog::Value::Null],
        catalog,
    );

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Null);
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_sql_function_is_setof() {
    let catalog = Arc::new(Catalog::new());

    let func_scalar = Function {
        name: "add".to_string(),
        parameters: vec![],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 + $2".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let executor = SqlFunctionExecutor::new(func_scalar, vec![], catalog.clone());
    assert!(!executor.is_setof());

    let func_setof = Function {
        name: "get_nums".to_string(),
        parameters: vec![],
        return_type: "SETOF INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT 1".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let executor = SqlFunctionExecutor::new(func_setof, vec![], catalog);
    assert!(executor.is_setof());
}

#[test]
fn test_sql_function_returning_table() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "get_pair".to_string(),
        parameters: vec![],
        return_type: "TABLE(a INT, b TEXT)".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT 1 AS a, 'hello' AS b".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let executor = SqlFunctionExecutor::new(func, vec![], catalog);
    assert!(executor.is_setof());
}

#[test]
fn test_sql_function_multiple_params() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "calculate".to_string(),
        parameters: vec![],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT ($1 + $2) * $3".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(
        func,
        vec![
            vaultgres::catalog::Value::Int(1),
            vaultgres::catalog::Value::Int(2),
            vaultgres::catalog::Value::Int(3),
        ],
        catalog,
    );

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Int(9));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_sql_function_constant_return() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "pi".to_string(),
        parameters: vec![],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT 42".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(func, vec![], catalog);

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Int(42));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_sql_function_bool_return() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "is_positive".to_string(),
        parameters: vec![],
        return_type: "BOOL".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 > 0".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor =
        SqlFunctionExecutor::new(func, vec![vaultgres::catalog::Value::Int(5)], catalog);

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Bool(true));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_parse_create_function() {
    let sql = "CREATE FUNCTION my_add(a INT, b INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + $2'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        vaultgres::parser::ast::Statement::CreateFunction(func) => {
            assert_eq!(func.name, "my_add");
            assert_eq!(func.parameters.len(), 2);
            assert_eq!(func.language, "SQL");
        }
        _ => panic!("Expected CreateFunction statement"),
    }
}

#[test]
fn test_parse_create_function_with_setof() {
    let sql = "CREATE FUNCTION get_nums() RETURNS SETOF INT LANGUAGE SQL AS 'SELECT 1'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        vaultgres::parser::ast::Statement::CreateFunction(func) => {
            assert_eq!(func.name, "get_nums");
            assert!(matches!(
                func.return_type,
                vaultgres::parser::ast::FunctionReturnType::Setof(ref s) if s == "INT"
            ));
        }
        _ => panic!("Expected CreateFunction statement"),
    }
}

#[test]
fn test_parse_create_function_with_table() {
    let sql = "CREATE FUNCTION get_rows() RETURNS TABLE(id INT, name TEXT) LANGUAGE SQL AS 'SELECT 1, ''test'''";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        vaultgres::parser::ast::Statement::CreateFunction(func) => {
            assert_eq!(func.name, "get_rows");
            assert!(matches!(
                func.return_type,
                vaultgres::parser::ast::FunctionReturnType::Table(ref cols) if cols.len() == 2
            ));
        }
        _ => panic!("Expected CreateFunction statement"),
    }
}

#[test]
fn test_parse_create_function_with_volatility() {
    let sql = "CREATE FUNCTION immutable_func() RETURNS INT LANGUAGE SQL IMMUTABLE AS 'SELECT 1'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    match stmt {
        vaultgres::parser::ast::Statement::CreateFunction(func) => {
            assert_eq!(func.name, "immutable_func");
            assert!(matches!(
                func.volatility,
                Some(vaultgres::parser::ast::FunctionVolatility::Immutable)
            ));
        }
        _ => panic!("Expected CreateFunction statement"),
    }
}

#[test]
fn test_sql_function_text_concat_with_quotes() {
    let catalog = Arc::new(Catalog::new());

    let func = Function {
        name: "escape_test".to_string(),
        parameters: vec![],
        return_type: "TEXT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT CONCAT($1, ' said hello')".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(
        func,
        vec![vaultgres::catalog::Value::Text("Alice".to_string())],
        catalog,
    );

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Text("Alice said hello".to_string()));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}

#[test]
fn test_sql_function_subquery() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "test_table".to_string(),
            vec![vaultgres::parser::ast::ColumnDef {
                name: "id".to_string(),
                data_type: vaultgres::parser::ast::DataType::Int,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            }],
        )
        .unwrap();

    for i in 1..=5 {
        catalog.insert("test_table", &[], vec![vaultgres::parser::ast::Expr::Number(i)]).unwrap();
    }

    let func = Function {
        name: "count_rows".to_string(),
        parameters: vec![],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT COUNT(*) FROM test_table".to_string(),
        is_variadic: false,
        volatility: vaultgres::catalog::FunctionVolatility::Stable,
        cost: 100.0,
        rows: 1,
    };

    let mut executor = SqlFunctionExecutor::new(func, vec![], catalog);

    match executor.next() {
        Ok(Some(tuple)) => {
            if let Some(value) = tuple.values().next() {
                assert_eq!(value, &vaultgres::catalog::Value::Int(5));
            }
        }
        Ok(None) => panic!("Expected a result"),
        Err(e) => panic!("Error: {:?}", e),
    }
}
