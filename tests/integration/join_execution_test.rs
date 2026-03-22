use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr, SelectStmt};
use vaultgres::parser::{Parser, Statement};

fn setup_catalog() -> Catalog {
    Catalog::new()
}

fn create_table(catalog: &Catalog, name: &str, columns: Vec<(&str, DataType)>) {
    catalog
        .create_table(
            name.to_string(),
            columns.iter().map(|(n, t)| ColumnDef::new(n.to_string(), t.clone())).collect(),
        )
        .unwrap();
}

fn parse_select(sql: &str) -> SelectStmt {
    let mut parser = Parser::new(sql).unwrap();
    match parser.parse().unwrap() {
        Statement::Select(s) => s,
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_inner_join_basic() {
    let catalog = setup_catalog();
    create_table(&catalog, "customers", vec![("id", DataType::Int), ("name", DataType::Text)]);
    create_table(
        &catalog,
        "orders",
        vec![("id", DataType::Int), ("customer_id", DataType::Int), ("total", DataType::Int)],
    );

    catalog.insert("customers", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("customers", &[], vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(1), Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(2), Expr::Number(1), Expr::Number(200)]).unwrap();

    let select = parse_select(
        "SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
    );
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.from, "customers");
}

#[test]
fn test_join_no_matches() {
    let catalog = setup_catalog();
    create_table(&catalog, "a", vec![("id", DataType::Int)]);
    create_table(&catalog, "b", vec![("a_id", DataType::Int)]);

    catalog.insert("a", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", &[], vec![Expr::Number(2)]).unwrap();

    let select = parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(select.joins.len(), 1);
}

#[test]
fn test_multiple_joins() {
    let catalog = setup_catalog();
    create_table(&catalog, "a", vec![("id", DataType::Int)]);
    create_table(&catalog, "b", vec![("id", DataType::Int), ("a_id", DataType::Int)]);
    create_table(&catalog, "c", vec![("b_id", DataType::Int)]);

    catalog.insert("a", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", &[], vec![Expr::Number(1), Expr::Number(1)]).unwrap();
    catalog.insert("c", &[], vec![Expr::Number(1)]).unwrap();

    let select = parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id");
    assert_eq!(select.joins.len(), 2);
}

#[test]
fn test_join_with_where_clause() {
    let catalog = setup_catalog();
    create_table(&catalog, "users", vec![("id", DataType::Int), ("status", DataType::Int)]);
    create_table(&catalog, "orders", vec![("user_id", DataType::Int), ("amount", DataType::Int)]);

    catalog.insert("users", &[], vec![Expr::Number(1), Expr::Number(1)]).unwrap();
    catalog.insert("users", &[], vec![Expr::Number(2), Expr::Number(0)]).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let select =
        parse_select("SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 1");
    assert!(select.where_clause.is_some());
    assert_eq!(select.joins.len(), 1);
}

#[test]
fn test_join_empty_tables() {
    let catalog = setup_catalog();
    create_table(&catalog, "a", vec![("id", DataType::Int)]);
    create_table(&catalog, "b", vec![("a_id", DataType::Int)]);

    let select = parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(select.joins.len(), 1);
}

#[test]
fn test_join_with_qualified_columns_in_select() {
    let catalog = setup_catalog();
    create_table(&catalog, "t1", vec![("id", DataType::Int), ("name", DataType::Text)]);
    create_table(&catalog, "t2", vec![("t1_id", DataType::Int), ("value", DataType::Int)]);

    catalog.insert("t1", &[], vec![Expr::Number(1), Expr::String("A".to_string())]).unwrap();
    catalog.insert("t2", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let select = parse_select("SELECT t1.name, t2.value FROM t1 JOIN t2 ON t1.id = t2.t1_id");
    assert_eq!(select.columns.len(), 2);
    assert!(matches!(select.columns[0], Expr::QualifiedColumn { .. }));
    assert!(matches!(select.columns[1], Expr::QualifiedColumn { .. }));
}

#[test]
fn test_join_same_column_names() {
    let catalog = setup_catalog();
    create_table(&catalog, "t1", vec![("id", DataType::Int), ("name", DataType::Text)]);
    create_table(&catalog, "t2", vec![("id", DataType::Int), ("name", DataType::Text)]);

    catalog.insert("t1", &[], vec![Expr::Number(1), Expr::String("A".to_string())]).unwrap();
    catalog.insert("t2", &[], vec![Expr::Number(1), Expr::String("B".to_string())]).unwrap();

    let select = parse_select("SELECT t1.name, t2.name FROM t1 JOIN t2 ON t1.id = t2.id");
    assert_eq!(select.columns.len(), 2);
}

#[test]
fn test_join_with_order_by() {
    let catalog = setup_catalog();
    create_table(&catalog, "a", vec![("id", DataType::Int), ("name", DataType::Text)]);
    create_table(&catalog, "b", vec![("a_id", DataType::Int)]);

    catalog.insert("a", &[], vec![Expr::Number(2), Expr::String("B".to_string())]).unwrap();
    catalog.insert("a", &[], vec![Expr::Number(1), Expr::String("A".to_string())]).unwrap();
    catalog.insert("b", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", &[], vec![Expr::Number(2)]).unwrap();

    let select = parse_select("SELECT a.name FROM a JOIN b ON a.id = b.a_id ORDER BY a.name");
    assert!(select.order_by.is_some());
}

#[test]
fn test_join_with_limit() {
    let catalog = setup_catalog();
    create_table(&catalog, "a", vec![("id", DataType::Int)]);
    create_table(&catalog, "b", vec![("a_id", DataType::Int)]);

    catalog.insert("a", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("a", &[], vec![Expr::Number(2)]).unwrap();
    catalog.insert("b", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", &[], vec![Expr::Number(2)]).unwrap();

    let select = parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id LIMIT 1");
    assert_eq!(select.limit, Some(1));
}

#[test]
fn test_join_all_types() {
    for (sql, join_type) in [
        ("SELECT * FROM a INNER JOIN b ON a.id = b.a_id", "INNER"),
        ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id", "LEFT"),
        ("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id", "RIGHT"),
        ("SELECT * FROM a FULL JOIN b ON a.id = b.a_id", "FULL"),
    ] {
        let select = parse_select(sql);
        assert_eq!(select.joins.len(), 1, "Failed for {}", join_type);
    }
}
