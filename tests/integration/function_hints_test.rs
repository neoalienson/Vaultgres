use rustgres::parser::Parser;

#[test]
fn test_function_with_cost_hint() {
    let sql = "CREATE FUNCTION expensive_func(a INT) RETURNS INT LANGUAGE SQL IMMUTABLE COST 1000 AS 'SELECT $1 * 2'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.name, "expensive_func");
        assert_eq!(func.cost, Some(1000.0));
        assert_eq!(func.rows, None);
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_with_rows_hint() {
    let sql = "CREATE FUNCTION generate_data() RETURNS SETOF INT LANGUAGE SQL STABLE ROWS 1000 AS 'SELECT generate_series(1, 1000)'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.name, "generate_data");
        assert_eq!(func.cost, None);
        assert_eq!(func.rows, Some(1000));
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_with_cost_and_rows() {
    let sql = "CREATE FUNCTION complex_query(n INT) RETURNS TABLE(id INT, value TEXT) LANGUAGE SQL VOLATILE COST 500 ROWS 100 AS 'SELECT id, value FROM data WHERE id < $1'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.name, "complex_query");
        assert_eq!(func.cost, Some(500.0));
        assert_eq!(func.rows, Some(100));
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_without_hints() {
    let sql = "CREATE FUNCTION simple_func(x INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + 1'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.name, "simple_func");
        assert_eq!(func.cost, None);
        assert_eq!(func.rows, None);
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_cost_hint_only() {
    let sql =
        "CREATE FUNCTION fast_func() RETURNS INT LANGUAGE SQL IMMUTABLE COST 1 AS 'SELECT 42'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.cost, Some(1.0));
        assert_eq!(func.rows, None);
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_rows_hint_only() {
    let sql = "CREATE FUNCTION get_users() RETURNS SETOF INT LANGUAGE SQL ROWS 50 AS 'SELECT id FROM users'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.cost, None);
        assert_eq!(func.rows, Some(50));
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_hints_with_volatility() {
    let sql = "CREATE FUNCTION cached_query(id INT) RETURNS TEXT LANGUAGE SQL STABLE COST 10 ROWS 1 AS 'SELECT name FROM cache WHERE id = $1'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.volatility, Some(rustgres::parser::ast::FunctionVolatility::Stable));
        assert_eq!(func.cost, Some(10.0));
        assert_eq!(func.rows, Some(1));
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_large_cost_value() {
    let sql = "CREATE FUNCTION very_expensive() RETURNS INT LANGUAGE SQL COST 1000000 AS 'SELECT COUNT(*) FROM huge_table'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.cost, Some(1000000.0));
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_function_large_rows_value() {
    let sql = "CREATE FUNCTION get_all_data() RETURNS SETOF INT LANGUAGE SQL ROWS 5000000 AS 'SELECT * FROM big_table'";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let rustgres::parser::ast::Statement::CreateFunction(func) = stmt {
        assert_eq!(func.rows, Some(5000000));
    } else {
        panic!("Expected CreateFunction statement");
    }
}
