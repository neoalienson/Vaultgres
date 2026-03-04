use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};

fn setup_catalog_with_data(
    columns: Vec<(&str, DataType)>,
    data: Vec<Vec<Expr>>,
) -> (Catalog, Arc<Catalog>) {
    let catalog = Catalog::new();
    let cols: Vec<ColumnDef> =
        columns.iter().map(|(n, t)| ColumnDef::new(n.to_string(), t.clone())).collect();
    catalog.create_table("data".to_string(), cols).unwrap();
    for row in data {
        catalog.insert("data", row).unwrap();
    }
    let catalog_arc = Arc::new(catalog.clone());
    (catalog, catalog_arc)
}

fn binary_op(col: &str, op: BinaryOperator, value: Expr) -> Expr {
    Expr::BinaryOp { left: Box::new(Expr::Column(col.to_string())), op, right: Box::new(value) }
}

fn select_where(catalog_arc: &Arc<Catalog>, where_clause: Expr) -> Vec<Vec<Value>> {
    Catalog::select_with_catalog(
        catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        Some(where_clause),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap()
}

#[test]
fn test_select_with_where() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("id", DataType::Int), ("value", DataType::Int)],
        vec![
            vec![Expr::Number(1), Expr::Number(100)],
            vec![Expr::Number(2), Expr::Number(200)],
            vec![Expr::Number(3), Expr::Number(300)],
        ],
    );

    let rows = select_where(&catalog_arc, binary_op("id", BinaryOperator::Equals, Expr::Number(2)));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(2));
    assert_eq!(rows[0][1], Value::Int(200));
}

#[test]
fn test_select_with_not_equals() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("id", DataType::Int)],
        vec![vec![Expr::Number(1)], vec![Expr::Number(2)], vec![Expr::Number(3)]],
    );

    let rows =
        select_where(&catalog_arc, binary_op("id", BinaryOperator::NotEquals, Expr::Number(2)));
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_with_less_than() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("value", DataType::Int)],
        vec![vec![Expr::Number(10)], vec![Expr::Number(20)], vec![Expr::Number(30)]],
    );

    let rows =
        select_where(&catalog_arc, binary_op("value", BinaryOperator::LessThan, Expr::Number(25)));
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_with_greater_than() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("value", DataType::Int)],
        vec![vec![Expr::Number(10)], vec![Expr::Number(20)], vec![Expr::Number(30)]],
    );

    let rows = select_where(
        &catalog_arc,
        binary_op("value", BinaryOperator::GreaterThan, Expr::Number(15)),
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_where_with_and() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("id", DataType::Int), ("value", DataType::Int)],
        vec![
            vec![Expr::Number(1), Expr::Number(10)],
            vec![Expr::Number(2), Expr::Number(20)],
            vec![Expr::Number(3), Expr::Number(30)],
        ],
    );

    let where_clause = Expr::BinaryOp {
        left: Box::new(binary_op("id", BinaryOperator::GreaterThan, Expr::Number(1))),
        op: BinaryOperator::And,
        right: Box::new(binary_op("value", BinaryOperator::LessThan, Expr::Number(30))),
    };

    let rows = select_where(&catalog_arc, where_clause);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(2));
}

#[test]
fn test_where_with_or() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("id", DataType::Int)],
        vec![vec![Expr::Number(1)], vec![Expr::Number(2)], vec![Expr::Number(3)]],
    );

    let where_clause = Expr::BinaryOp {
        left: Box::new(binary_op("id", BinaryOperator::Equals, Expr::Number(1))),
        op: BinaryOperator::Or,
        right: Box::new(binary_op("id", BinaryOperator::Equals, Expr::Number(3))),
    };

    let rows = select_where(&catalog_arc, where_clause);
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_like_operator() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("name", DataType::Text)],
        vec![
            vec![Expr::String("hello world".to_string())],
            vec![Expr::String("goodbye".to_string())],
            vec![Expr::String("hello there".to_string())],
        ],
    );

    let rows = select_where(
        &catalog_arc,
        binary_op("name", BinaryOperator::Like, Expr::String("%hello%".to_string())),
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_in_operator() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("id", DataType::Int)],
        vec![
            vec![Expr::Number(1)],
            vec![Expr::Number(2)],
            vec![Expr::Number(3)],
            vec![Expr::Number(4)],
        ],
    );

    let rows = select_where(
        &catalog_arc,
        binary_op("id", BinaryOperator::In, Expr::List(vec![Expr::Number(1), Expr::Number(3)])),
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_between_operator() {
    let (catalog, catalog_arc) = setup_catalog_with_data(
        vec![("value", DataType::Int)],
        vec![
            vec![Expr::Number(5)],
            vec![Expr::Number(15)],
            vec![Expr::Number(25)],
            vec![Expr::Number(35)],
        ],
    );

    // Use parser to create BETWEEN expression (which converts to AND of comparisons)
    let where_clause = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(10)),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(Expr::Number(30)),
        }),
    };

    let rows = select_where(&catalog_arc, where_clause);
    assert_eq!(rows.len(), 2);
}
