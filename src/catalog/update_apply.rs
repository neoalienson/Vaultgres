//! UPDATE assignment logic
//!
//! Handles applying assignment expressions to tuples during UPDATE operations.

use super::{Catalog, TableSchema, Tuple, Value};
use crate::catalog::predicate::PredicateEvaluator;
use crate::catalog::select_executor::SelectExecutor;
use crate::executor::Eval;
use crate::executor::expr_evaluator::{eval_binary_op, eval_unary_op};
use crate::parser::ast::Expr;
use crate::transaction::{Snapshot, TransactionManager};
use std::collections::HashMap;
use std::sync::Arc;

use super::update_validation::validate_assignment_type;

/// Apply assignments to a tuple
pub fn apply_assignments(
    tuple: &mut Tuple,
    assignments: &[(String, Expr)],
    schema: &TableSchema,
    catalog: &Catalog,
) -> Result<(), String> {
    for (col_name, expr) in assignments {
        let idx = schema
            .columns
            .iter()
            .position(|c| &c.name == col_name)
            .ok_or_else(|| format!("Column '{}' not found", col_name))?;

        let value = evaluate_expr(expr, &tuple.data, schema, catalog)?;

        validate_assignment_type(&schema.columns[idx].data_type, &value, col_name)?;
        tuple.data[idx] = value;
    }
    Ok(())
}

/// Apply assignments with subquery support
pub fn apply_assignments_with_tuples(
    tuple: &mut Tuple,
    assignments: &[(String, Expr)],
    schema: &TableSchema,
    catalog: &Catalog,
    subquery_tuples: &[Tuple],
    snapshot: &Snapshot,
) -> Result<(), String> {
    for (col_name, expr) in assignments {
        let idx = schema
            .columns
            .iter()
            .position(|c| &c.name == col_name)
            .ok_or_else(|| format!("Column '{}' not found", col_name))?;

        let value = evaluate_expr_with_tuples(
            expr,
            &tuple.data,
            schema,
            catalog,
            subquery_tuples,
            snapshot,
        )?;

        validate_assignment_type(&schema.columns[idx].data_type, &value, col_name)?;
        tuple.data[idx] = value;
    }
    Ok(())
}

/// Evaluate an expression against tuple data
pub fn evaluate_expr(
    expr: &Expr,
    tuple_data: &[Value],
    schema: &TableSchema,
    catalog: &Catalog,
) -> Result<Value, String> {
    let tuple = build_tuple(tuple_data, schema);
    Eval::eval_expr_with_catalog(expr, &tuple, Some(catalog), None, None)
        .map_err(|e| format!("{}", e))
}

/// Build a tuple HashMap from tuple data and schema
pub fn build_tuple(tuple_data: &[Value], schema: &TableSchema) -> HashMap<String, Value> {
    let mut tuple = HashMap::new();
    for (idx, col) in schema.columns.iter().enumerate() {
        if idx < tuple_data.len() {
            tuple.insert(col.name.clone(), tuple_data[idx].clone());
        }
    }
    tuple
}

/// Evaluate an expression that may include subqueries
pub fn evaluate_expr_with_tuples(
    expr: &Expr,
    tuple_data: &[Value],
    schema: &TableSchema,
    catalog: &Catalog,
    subquery_tuples: &[Tuple],
    snapshot: &Snapshot,
) -> Result<Value, String> {
    match expr {
        Expr::Number(n) => Ok(Value::Int(*n)),
        Expr::Float(f) => Ok(Value::Float(*f)),
        Expr::String(s) => Ok(Value::Text(s.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::Column(name) => {
            let lookup_name =
                if let Some(dot_pos) = name.find('.') { &name[dot_pos + 1..] } else { name };
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == lookup_name)
                .ok_or_else(|| format!("Column '{}' not found", name))?;
            Ok(tuple_data[idx].clone())
        }
        Expr::QualifiedColumn { table: _, column } => {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == column)
                .ok_or_else(|| format!("Column '{}' not found", column))?;
            Ok(tuple_data[idx].clone())
        }
        Expr::Subquery(subquery) => {
            match SelectExecutor::eval_scalar_subquery_with_tuples(
                catalog,
                subquery,
                subquery_tuples,
                snapshot,
            ) {
                Ok(value) => Ok(value),
                Err(_) => Ok(Value::Null),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expr_with_tuples(
                left,
                tuple_data,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            let right_val = evaluate_expr_with_tuples(
                right,
                tuple_data,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            eval_binary_op(&left_val, op, &right_val)
        }
        Expr::Case { conditions, else_expr } => {
            for (when_expr, then_expr) in conditions {
                let when_val = evaluate_expr_with_tuples(
                    when_expr,
                    tuple_data,
                    schema,
                    catalog,
                    subquery_tuples,
                    snapshot,
                )?;
                if when_val == Value::Bool(true) {
                    return evaluate_expr_with_tuples(
                        then_expr,
                        tuple_data,
                        schema,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    );
                }
            }
            if let Some(else_expr) = else_expr {
                return evaluate_expr_with_tuples(
                    else_expr,
                    tuple_data,
                    schema,
                    catalog,
                    subquery_tuples,
                    snapshot,
                );
            }
            Ok(Value::Null)
        }
        Expr::UnaryOp { op, expr } => {
            let val = evaluate_expr_with_tuples(
                expr,
                tuple_data,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            eval_unary_op(op, &val)
        }
        Expr::IsNull(expr) => {
            let val = evaluate_expr_with_tuples(
                expr,
                tuple_data,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            Ok(Value::Bool(matches!(val, Value::Null)))
        }
        Expr::IsNotNull(expr) => {
            let val = evaluate_expr_with_tuples(
                expr,
                tuple_data,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            Ok(Value::Bool(!matches!(val, Value::Null)))
        }
        _ => {
            let tuple = build_tuple(tuple_data, schema);
            Eval::eval_expr_with_catalog(expr, &tuple, Some(catalog), None, None)
                .map_err(|e| format!("{}", e))
        }
    }
}

/// Evaluate a function call
pub fn eval_function(name: &str, args: Vec<Value>, catalog: &Catalog) -> Result<Value, String> {
    Eval::eval_function_call(name, args, Some(catalog)).map_err(|e| format!("{}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::TableSchema as TestSchema;
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};
    use crate::transaction::{Snapshot, TransactionManager, TupleHeader};
    use std::collections::HashMap;

    fn create_test_schema() -> TestSchema {
        TestSchema::new(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("age".to_string(), DataType::Int),
            ],
        )
    }

    fn create_test_tuple(xmin: u64, id: i64, name: &str, age: i64) -> Tuple {
        let mut tuple =
            Tuple { header: TupleHeader::new(xmin), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(id));
        tuple.add_value("name".to_string(), Value::Text(name.to_string()));
        tuple.add_value("age".to_string(), Value::Int(age));
        tuple
    }

    #[test]
    fn test_apply_assignments_success() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![
            ("name".to_string(), Expr::String("Bob".to_string())),
            ("age".to_string(), Expr::Number(31)),
        ];

        assert!(apply_assignments(&mut tuple, &assignments, &schema, &Catalog::new()).is_ok());
        assert_eq!(tuple.get_value("name"), Some(Value::Text("Bob".to_string())));
        assert_eq!(tuple.get_value("age"), Some(Value::Int(31)));
    }

    #[test]
    fn test_apply_assignments_column_not_found() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("non_existent".to_string(), Expr::Number(100))];

        let result = apply_assignments(&mut tuple, &assignments, &schema, &Catalog::new());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Column 'non_existent' not found");
    }

    #[test]
    fn test_apply_assignments_type_mismatch() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("name".to_string(), Expr::Number(123))];

        let result = apply_assignments(&mut tuple, &assignments, &schema, &Catalog::new());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Type mismatch"));
    }

    #[test]
    fn test_apply_assignments_column_reference() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("age".to_string(), Expr::Column("id".to_string()))];

        assert!(apply_assignments(&mut tuple, &assignments, &schema, &Catalog::new()).is_ok());
        assert_eq!(tuple.get_value("age"), Some(Value::Int(1)));
    }

    #[test]
    fn test_apply_assignments_arithmetic() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(1)),
            },
        )];

        assert!(apply_assignments(&mut tuple, &assignments, &schema, &Catalog::new()).is_ok());
        assert_eq!(tuple.get_value("age"), Some(Value::Int(31)));
    }
}
