use std::sync::Arc;
#[test]
fn test_subquery_with_avg_aggregate() {
    env_logger::builder().filter_level(log::LevelFilter::Trace).is_test(true).try_init().ok();

    use vaultgres::catalog::Catalog;
    use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());

    // Create table
    catalog
        .create_table(
            "items".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("price".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    // Insert data
    catalog.insert("items", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("items", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
    catalog.insert("items", vec![Expr::Number(3), Expr::Number(300)]).unwrap();

    // Build WHERE clause: price > (SELECT AVG(price) FROM items)
    use vaultgres::parser::ast::{AggregateFunc, BinaryOperator, SelectStmt};
    let where_clause = Expr::BinaryOp {
        left: Box::new(Expr::Column("price".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Subquery(Box::new(SelectStmt {
            columns: vec![Expr::Aggregate {
                func: AggregateFunc::Avg,
                arg: Box::new(Expr::Column("price".to_string())),
            }],
            from: "items".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            distinct: false,
        }))),
    };

    // Query with subquery - AVG(price) = 200, so only price=300 should be returned
    let result = Catalog::select_with_catalog(&catalog_arc, 
        "items",
        false,
        vec!["id".to_string(), "price".to_string()],
        Some(where_clause),
        None,
        None,
        None,
        None,
        None,
    );

    assert!(result.is_ok(), "Query failed: {:?}", result.err());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1, "Expected 1 row, got {}", rows.len());
    assert_eq!(rows[0][1], vaultgres::catalog::Value::Int(300));
}
