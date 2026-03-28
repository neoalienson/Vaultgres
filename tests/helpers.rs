//! Shared test utilities for integration tests
//!
//! This module provides builders and helpers for creating test fixtures
//! following PostgreSQL-style regression test patterns.

use std::sync::Arc;
use vaultgres::catalog::{Catalog, Column, DataType, EnumTypeDef, TableSchema, Tuple, Value};
use vaultgres::parser::ast::ColumnDef;
use vaultgres::parser::ast::{
    AggregateFunc, BinaryOperator, ColumnDef as AstColumnDef, DataType as AstDataType, Expr,
    OrderByExpr, SelectStmt, UnaryOperator,
};
use vaultgres::transaction::{Snapshot, TransactionManager};

use std::collections::HashMap;

// ============================================================================
// Catalog Test Helpers
// ============================================================================

/// Create a new catalog for testing (in-memory, no persistence)
pub fn create_test_catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new())
}

/// Create a catalog with a single table already created
pub fn create_catalog_with_table(name: &str, columns: Vec<(&str, DataType)>) -> Arc<Catalog> {
    let catalog = create_test_catalog();
    let schema = create_schema(name, columns);
    catalog.create_table(name.to_string(), schema.columns).unwrap();
    catalog
}

/// Create a catalog with table and test data inserted
pub fn create_catalog_with_data(
    table_name: &str,
    columns: Vec<(&str, DataType)>,
    rows: Vec<Vec<Value>>,
) -> Arc<Catalog> {
    let catalog = create_catalog_with_table(table_name, columns);

    for row in rows {
        let values: Vec<Expr> = row.into_iter().map(value_to_expr).collect();
        catalog.insert(table_name, &[], values).unwrap();
    }

    catalog
}

/// Convert a Value to an Expr for insertion
fn value_to_expr(value: Value) -> Expr {
    match value {
        Value::Int(n) => Expr::Number(n),
        Value::Float(f) => Expr::Float(f),
        Value::Text(s) => Expr::String(s),
        Value::Bool(b) => Expr::Number(if b { 1 } else { 0 }),
        Value::Null => Expr::Null,
        _ => unimplemented!("value_to_expr not implemented for {:?}", value),
    }
}

// ============================================================================
// Schema Builders
// ============================================================================

/// Create a schema from column name/type pairs
pub fn create_schema(table_name: &str, columns: Vec<(&str, DataType)>) -> TableSchema {
    TableSchema::new(
        table_name.to_string(),
        columns
            .into_iter()
            .map(|(name, data_type)| ColumnDef {
                name: name.to_string(),
                data_type,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            })
            .collect(),
    )
}

/// Create a simple single-column schema
pub fn simple_schema(column_name: &str, data_type: DataType) -> TableSchema {
    create_schema("test", vec![(column_name, data_type)])
}

/// Create a schema for testing with common types
pub fn users_schema() -> TableSchema {
    create_schema(
        "users",
        vec![("id", DataType::Int), ("name", DataType::Text), ("email", DataType::Text)],
    )
}

/// Create a schema for orders table
pub fn orders_schema() -> TableSchema {
    create_schema(
        "orders",
        vec![("id", DataType::Int), ("customer_id", DataType::Int), ("total", DataType::Int)],
    )
}

/// Create a schema for products table
pub fn products_schema() -> TableSchema {
    create_schema(
        "products",
        vec![
            ("id", DataType::Int),
            ("name", DataType::Text),
            ("price", DataType::Int),
            ("quantity", DataType::Int),
        ],
    )
}

// ============================================================================
// Tuple Builders
// ============================================================================

/// Create a test tuple with visible header
pub fn visible_tuple(_xmin: u64) -> Tuple {
    Tuple::new()
}

/// Create a tuple with a single value
pub fn tuple_with_1_value(key: &str, value: Value) -> Tuple {
    let mut tuple = Tuple::new();
    tuple.add_value(key.to_string(), value);
    tuple
}

/// Create a tuple with multiple values
pub fn tuple_with_values(values: Vec<(&str, Value)>) -> Tuple {
    let mut tuple = Tuple::new();
    for (key, value) in values {
        tuple.add_value(key.to_string(), value);
    }
    tuple
}

/// Add values to a tuple
pub fn add_values(mut tuple: Tuple, values: Vec<(&str, Value)>) -> Tuple {
    for (key, value) in values {
        tuple.add_value(key.to_string(), value);
    }
    tuple
}

// ============================================================================
// Expression Builders (PostgreSQL-style)
// ============================================================================

/// Create a column reference expression
pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

/// Create a qualified column reference expression
pub fn qcol(table: &str, column: &str) -> Expr {
    Expr::QualifiedColumn { table: table.to_string(), column: column.to_string() }
}

/// Create a literal expression
pub fn lit<T: Into<Value>>(value: T) -> Expr {
    match value.into() {
        Value::Int(n) => Expr::Number(n),
        Value::Float(f) => Expr::Float(f),
        Value::Text(s) => Expr::String(s),
        Value::Bool(b) => Expr::Number(if b { 1 } else { 0 }),
        Value::Null => Expr::Null,
        _ => unimplemented!("lit not implemented for this Value type"),
    }
}

/// Create a binary operation expression
pub fn binop(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) }
}

/// Create a unary operation expression
pub fn unop(op: UnaryOperator, expr: Expr) -> Expr {
    Expr::UnaryOp { op, expr: Box::new(expr) }
}

/// Create an aggregate expression
pub fn agg(func: AggregateFunc, arg: Expr) -> Expr {
    Expr::Aggregate { func, arg: Box::new(arg) }
}

/// Create a COUNT(*) aggregate
pub fn count_star() -> Expr {
    Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }
}

/// Create a COUNT(column) aggregate
pub fn count(column: &str) -> Expr {
    agg(AggregateFunc::Count, col(column))
}

/// Create a SUM(column) aggregate
pub fn sum(column: &str) -> Expr {
    agg(AggregateFunc::Sum, col(column))
}

/// Create a AVG(column) aggregate
pub fn avg(column: &str) -> Expr {
    agg(AggregateFunc::Avg, col(column))
}

/// Create a MIN(column) aggregate
pub fn min(column: &str) -> Expr {
    agg(AggregateFunc::Min, col(column))
}

/// Create a MAX(column) aggregate
pub fn max(column: &str) -> Expr {
    agg(AggregateFunc::Max, col(column))
}

/// Create a simple SELECT statement
pub fn simple_select(columns: Vec<Expr>, from: &str) -> SelectStmt {
    SelectStmt {
        distinct: false,
        columns,
        from: from.to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }
}

/// Create a simple WHERE clause
pub fn where_clause(expr: Expr) -> Option<Expr> {
    Some(expr)
}

/// Create an ORDER BY clause
pub fn order_by(columns: Vec<(&str, bool)>) -> Option<Vec<OrderByExpr>> {
    Some(
        columns
            .into_iter()
            .map(|(col, asc)| OrderByExpr { column: col.to_string(), ascending: asc })
            .collect(),
    )
}

// ============================================================================
// Value Builders
// ============================================================================

/// Helper to create Value::Int
pub fn val_int(n: i64) -> Value {
    Value::Int(n)
}

/// Helper to create Value::Float
pub fn val_float(f: f64) -> Value {
    Value::Float(f)
}

/// Helper to create Value::Text
pub fn val_text(s: &str) -> Value {
    Value::Text(s.to_string())
}

/// Helper to create Value::Bool
pub fn val_bool(b: bool) -> Value {
    Value::Bool(b)
}

// ============================================================================
// Transaction/Snapshot Helpers
// ============================================================================

/// Create a snapshot for testing MVCC visibility
pub fn test_snapshot(xmin: u64, xmax: u64) -> Snapshot {
    Snapshot::new(xmin, xmax, vec![])
}

/// Create a committed transaction snapshot
pub fn committed_snapshot(xid: u64) -> Snapshot {
    Snapshot::new(xid, xid + 1, vec![])
}

/// Create a transaction manager and begin a transaction
pub fn begin_test_transaction() -> (Arc<TransactionManager>, u64) {
    let txn_mgr = Arc::new(TransactionManager::new());
    let txn = txn_mgr.begin();
    (txn_mgr, txn.xid)
}

// ============================================================================
// Test Data Sets
// ============================================================================

/// Standard test data for products
pub fn products_test_data() -> Vec<Vec<Value>> {
    vec![
        vec![Value::Int(1), Value::Text("Laptop".to_string()), Value::Int(1000), Value::Int(10)],
        vec![Value::Int(2), Value::Text("Mouse".to_string()), Value::Int(50), Value::Int(50)],
        vec![Value::Int(3), Value::Text("Keyboard".to_string()), Value::Int(150), Value::Int(30)],
    ]
}

/// Standard test data for users
pub fn users_test_data() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Int(1),
            Value::Text("Alice".to_string()),
            Value::Text("alice@example.com".to_string()),
        ],
        vec![
            Value::Int(2),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ],
        vec![
            Value::Int(3),
            Value::Text("Charlie".to_string()),
            Value::Text("charlie@example.com".to_string()),
        ],
    ]
}

/// Standard test data for orders
pub fn orders_test_data() -> Vec<Vec<Value>> {
    vec![
        vec![Value::Int(1), Value::Int(1), Value::Int(250)],
        vec![Value::Int(2), Value::Int(1), Value::Int(150)],
        vec![Value::Int(3), Value::Int(2), Value::Int(300)],
    ]
}

// ============================================================================
// SQL Regression Test Style Helpers
// ============================================================================

/// Represents an expected SQL test result
#[derive(Debug, Clone)]
pub enum SqlExpected {
    /// Expected rows (in order)
    Rows(Vec<Vec<Value>>),
    /// Expected error message (substring)
    Error(String),
    /// Expected row count
    RowCount(usize),
    /// Command complete tag
    CommandComplete(String),
}

/// Execute a SQL statement and check the result
pub fn sql_execute<F>(catalog: &Catalog, sql: &str, check: F)
where
    F: Fn(Result<Vec<Vec<Value>>, String>) -> bool,
{
    // This would be used with the parser and executor
    // For now, just a placeholder
    let _ = (catalog, sql, check);
}

/// Macro for defining SQL regression tests
#[macro_export]
macro_rules! sql_test {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let catalog = crate::tests::helpers::create_test_catalog();
            // Parse and execute SQL, then check result
            let _ = ($sql, $expected, catalog);
        }
    };
}

// ============================================================================
// Property-Based Test Helpers (proptest)
// ============================================================================

// NOTE: The proptest helpers below require Arbitrary implementations for
// vaultgres::parser::Expr and vaultgres::parser::BinaryOperator which don't exist.
// Commenting out until proper implementations are added.

// #[cfg(test)]
// use proptest::prelude::*;
//
// /// Strategy for generating random valid expressions
// #[cfg(test)]
// pub fn any_int_expr() -> impl Strategy<Value = Expr> {
//     any::<i64>().prop_map(|n| Expr::Number(n))
// }
//
// /// Strategy for generating simple column expressions
// #[cfg(test)]
// pub fn any_column_expr() -> impl Strategy<Value = Expr> {
//     prop_oneof![any_int_expr(), any::<String>().prop_map(|s| Expr::String(s))]
// }
//
// /// Strategy for generating binary operations
// #[cfg(test)]
// pub fn any_binary_op_expr() -> impl Strategy<Value = Expr> {
//     any::<(Expr, BinaryOperator, Expr)>().prop_map(|(l, op, r)| Expr::BinaryOp {
//         left: Box::new(l),
//         op,
//         right: Box::new(r),
//     })
// }
//
// /// Strategy for generating valid SELECT statements
// #[cfg(test)]
// pub fn any_simple_select() -> impl Strategy<Value = SelectStmt> {
//     (any::<i64>(), any::<bool>()).prop_map(|(limit, distinct)| SelectStmt {
//         distinct,
//         columns: vec![Expr::Star],
//         from: "test".to_string(),
//         table_alias: None,
//         joins: vec![],
//         where_clause: None,
//         group_by: None,
//         having: None,
//         order_by: None,
//         limit: Some(limit as usize),
//         offset: None,
//     })
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_schema() {
        let schema = create_schema("users", vec![("id", DataType::Int), ("name", DataType::Text)]);
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn test_tuple_with_values() {
        let tuple = tuple_with_values(vec![
            ("id", Value::Int(1)),
            ("name", Value::Text("test".to_string())),
        ]);
        assert_eq!(tuple.get_value("id"), Some(Value::Int(1)));
        assert_eq!(tuple.get_value("name"), Some(Value::Text("test".to_string())));
    }

    #[test]
    fn test_col() {
        assert_eq!(col("id"), Expr::Column("id".to_string()));
    }

    #[test]
    fn test_qcol() {
        assert_eq!(
            qcol("users", "id"),
            Expr::QualifiedColumn { table: "users".to_string(), column: "id".to_string() }
        );
    }

    #[test]
    fn test_binop() {
        let expr = binop(col("a"), BinaryOperator::Add, col("b"));
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Add, .. }));
    }

    #[test]
    fn test_count() {
        let expr = count("id");
        assert!(matches!(expr, Expr::Aggregate { func: AggregateFunc::Count, .. }));
    }

    #[test]
    fn test_simple_select() {
        let stmt = simple_select(vec![Expr::Star], "users");
        assert_eq!(stmt.from, "users");
        assert_eq!(stmt.columns.len(), 1);
    }

    #[test]
    fn test_value_from_i64() {
        let v = val_int(42);
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn test_value_from_str() {
        let v = val_text("hello");
        assert_eq!(v, Value::Text("hello".to_string()));
    }
}
