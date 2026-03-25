use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{AggregateFunc, ColumnDef, DataType, Expr};

#[test]
fn test_aggregate_count() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(3)]).unwrap();

    let agg_expr = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Count,
        arg: Box::new(Expr::Number(1)),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(3));
}

#[test]
fn test_aggregate_sum() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(20)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(30)]).unwrap();

    let agg_expr = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(60));
}

#[test]
fn test_aggregate_avg() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(20)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(30)]).unwrap();

    let agg_expr = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Avg,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(20));
}

#[test]
fn test_aggregate_min_max() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(50)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(30)]).unwrap();

    let agg_expr_min = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Min,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr_min],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows[0][0], Value::Int(10));

    let agg_expr_max = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Max,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr_max],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows[0][0], Value::Int(50));
}

#[test]
fn test_group_by() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::String("A".to_string()), Expr::Number(10)]).unwrap();
    catalog.insert("data", &[], vec![Expr::String("B".to_string()), Expr::Number(20)]).unwrap();
    catalog.insert("data", &[], vec![Expr::String("A".to_string()), Expr::Number(30)]).unwrap();
    catalog.insert("data", &[], vec![Expr::String("B".to_string()), Expr::Number(40)]).unwrap();

    let group_by = Some(vec![Expr::Column("category".to_string())]);
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Column("category".to_string())],
        None,
        group_by,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 2);
}

#[test]
fn test_having_clause() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::String("A".to_string()), Expr::Number(10)]).unwrap();
    catalog.insert("data", &[], vec![Expr::String("B".to_string()), Expr::Number(20)]).unwrap();
    catalog.insert("data", &[], vec![Expr::String("A".to_string()), Expr::Number(30)]).unwrap();
    catalog.insert("data", &[], vec![Expr::String("C".to_string()), Expr::Number(5)]).unwrap();

    let group_by = Some(vec![Expr::Column("category".to_string())]);
    let having = Some(Expr::BinaryOp {
        left: Box::new(Expr::Number(2)),
        op: vaultgres::parser::ast::BinaryOperator::GreaterThan,
        right: Box::new(Expr::Number(1)),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Column("category".to_string())],
        None,
        group_by,
        having,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_create_and_get_aggregate() {
    let catalog = Catalog::new();

    let agg = Aggregate {
        name: "my_avg".to_string(),
        input_type: "INT".to_string(),
        sfunc: "int8_avg_accum".to_string(),
        stype: "INT8".to_string(),
        finalfunc: Some("int8_avg".to_string()),
        initcond: None,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
    };

    catalog.create_aggregate(agg).unwrap();

    let retrieved = catalog.get_aggregate("my_avg").unwrap();
    assert_eq!(retrieved.name, "my_avg");
    assert_eq!(retrieved.input_type, "INT");
    assert_eq!(retrieved.sfunc, "int8_avg_accum");
    assert_eq!(retrieved.stype, "INT8");
    assert_eq!(retrieved.finalfunc, Some("int8_avg".to_string()));
}

#[test]
fn test_drop_aggregate() {
    let catalog = Catalog::new();

    let agg = Aggregate {
        name: "my_sum".to_string(),
        input_type: "INT".to_string(),
        sfunc: "int8_sum".to_string(),
        stype: "INT8".to_string(),
        finalfunc: None,
        initcond: Some("0".to_string()),
        volatility: FunctionVolatility::Volatile,
        cost: 100.0,
    };

    catalog.create_aggregate(agg).unwrap();
    assert!(catalog.get_aggregate("my_sum").is_some());

    catalog.drop_aggregate("my_sum", false).unwrap();
    assert!(catalog.get_aggregate("my_sum").is_none());
}

#[test]
fn test_drop_aggregate_if_exists() {
    let catalog = Catalog::new();

    let result = catalog.drop_aggregate("nonexistent", true);
    assert!(result.is_ok());

    let result = catalog.drop_aggregate("nonexistent", false);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "Aggregate 'nonexistent' does not exist");
}

#[test]
fn test_create_aggregate_with_parse() {
    let sql = "CREATE AGGREGATE my_count (INT) (SFUNC = int8_avg_accum, STYPE = INT8)";
    let stmt = vaultgres::parser::parse(sql).unwrap();

    match stmt {
        vaultgres::parser::ast::Statement::CreateAggregate(create) => {
            assert_eq!(create.name, "my_count");
            assert_eq!(create.input_type, "INT");
            assert_eq!(create.sfunc, "int8_avg_accum");
            assert_eq!(create.stype, "INT8");
        }
        _ => panic!("Expected CREATE AGGREGATE statement"),
    }
}

#[test]
fn test_drop_aggregate_with_parse() {
    let sql = "DROP AGGREGATE IF EXISTS my_count";
    let stmt = vaultgres::parser::parse(sql).unwrap();

    match stmt {
        vaultgres::parser::ast::Statement::DropAggregate(drop) => {
            assert_eq!(drop.name, "my_count");
            assert!(drop.if_exists);
        }
        _ => panic!("Expected DROP AGGREGATE statement"),
    }
}

#[test]
fn test_multiple_aggregates() {
    let catalog = Catalog::new();

    let agg1 = Aggregate {
        name: "my_avg".to_string(),
        input_type: "INT".to_string(),
        sfunc: "int8_avg_accum".to_string(),
        stype: "INT8".to_string(),
        finalfunc: Some("int8_avg".to_string()),
        initcond: None,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
    };

    let agg2 = Aggregate {
        name: "my_sum".to_string(),
        input_type: "INT".to_string(),
        sfunc: "int8_sum".to_string(),
        stype: "INT8".to_string(),
        finalfunc: None,
        initcond: None,
        volatility: FunctionVolatility::Volatile,
        cost: 100.0,
    };

    catalog.create_aggregate(agg1).unwrap();
    catalog.create_aggregate(agg2).unwrap();

    assert!(catalog.get_aggregate("my_avg").is_some());
    assert!(catalog.get_aggregate("my_sum").is_some());
    assert!(catalog.get_aggregate("my_count").is_none());
}

#[test]
fn test_aggregate_overwrite() {
    let catalog = Catalog::new();

    let agg1 = Aggregate {
        name: "my_agg".to_string(),
        input_type: "INT".to_string(),
        sfunc: "func_v1".to_string(),
        stype: "INT8".to_string(),
        finalfunc: None,
        initcond: None,
        volatility: FunctionVolatility::Volatile,
        cost: 100.0,
    };

    let agg2 = Aggregate {
        name: "my_agg".to_string(),
        input_type: "INT".to_string(),
        sfunc: "func_v2".to_string(),
        stype: "INT8".to_string(),
        finalfunc: None,
        initcond: None,
        volatility: FunctionVolatility::Volatile,
        cost: 100.0,
    };

    catalog.create_aggregate(agg1).unwrap();
    catalog.create_aggregate(agg2).unwrap();

    let retrieved = catalog.get_aggregate("my_agg").unwrap();
    assert_eq!(retrieved.sfunc, "func_v2");
}

#[test]
fn test_aggregate_volatility() {
    let catalog = Catalog::new();

    let agg_immutable = Aggregate {
        name: "my_avg".to_string(),
        input_type: "INT".to_string(),
        sfunc: "int8_avg_accum".to_string(),
        stype: "INT8".to_string(),
        finalfunc: Some("int8_avg".to_string()),
        initcond: None,
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
    };

    catalog.create_aggregate(agg_immutable).unwrap();

    let retrieved = catalog.get_aggregate("my_avg").unwrap();
    assert_eq!(retrieved.volatility, FunctionVolatility::Immutable);
}

#[test]
fn test_aggregate_with_initcond() {
    let catalog = Catalog::new();

    let agg = Aggregate {
        name: "my_concat".to_string(),
        input_type: "TEXT".to_string(),
        sfunc: "text_concat".to_string(),
        stype: "TEXT".to_string(),
        finalfunc: None,
        initcond: Some("''".to_string()),
        volatility: FunctionVolatility::Immutable,
        cost: 100.0,
    };

    catalog.create_aggregate(agg).unwrap();

    let retrieved = catalog.get_aggregate("my_concat").unwrap();
    assert_eq!(retrieved.initcond, Some("''".to_string()));
}
