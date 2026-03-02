use crate::catalog::*;
use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};

#[test]
fn test_select_with_where() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(2)),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(2));
    assert_eq!(rows[0][1], Value::Int(200));
}

#[test]
fn test_select_with_not_equals() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::NotEquals,
        right: Box::new(Expr::Number(2)),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_with_less_than() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::Number(30)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("value".to_string())),
        op: BinaryOperator::LessThan,
        right: Box::new(Expr::Number(25)),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_with_greater_than() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::Number(30)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("value".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Number(15)),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_where_with_and() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1), Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2), Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3), Expr::Number(30)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(1)),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(30)),
        }),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(2));
}

#[test]
fn test_where_with_or() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        }),
        op: BinaryOperator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(3)),
        }),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_like_operator() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("name".to_string(), DataType::Text)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::String("hello world".to_string())]).unwrap();
    catalog.insert("data", vec![Expr::String("goodbye".to_string())]).unwrap();
    catalog.insert("data", vec![Expr::String("hello there".to_string())]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("name".to_string())),
        op: BinaryOperator::Like,
        right: Box::new(Expr::String("%hello%".to_string())),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_in_operator() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();
    catalog.insert("data", vec![Expr::Number(4)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::In,
        right: Box::new(Expr::List(vec![Expr::Number(1), Expr::Number(3)])),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_between_operator() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(5)]).unwrap();
    catalog.insert("data", vec![Expr::Number(15)]).unwrap();
    catalog.insert("data", vec![Expr::Number(25)]).unwrap();
    catalog.insert("data", vec![Expr::Number(35)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("value".to_string())),
        op: BinaryOperator::Between,
        right: Box::new(Expr::List(vec![Expr::Number(10), Expr::Number(30)])),
    });

    let rows = catalog
        .select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None)
        .unwrap();
    assert_eq!(rows.len(), 2);
}
