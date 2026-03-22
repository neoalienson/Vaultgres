use super::{Catalog, TableSchema, Tuple, Value};
use crate::catalog::predicate::PredicateEvaluator;
use crate::catalog::select_executor::SelectExecutor;
use crate::executor::expr_evaluator::{eval_binary_op, eval_unary_op};
use crate::parser::ast::{DataType, Expr};
use crate::transaction::{Snapshot, TransactionManager};
use std::sync::Arc;

pub struct UpdateDeleteExecutor;

impl UpdateDeleteExecutor {
    pub fn update(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        catalog: &Catalog,
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            Self::apply_assignments(tuple, assignments, schema, catalog)?;
            updated += 1;
        }
        Ok(updated)
    }

    pub fn update_with_tuples(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        catalog: &Catalog,
        subquery_tuples: &[Tuple],
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            Self::apply_assignments_with_tuples(
                tuple,
                assignments,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            updated += 1;
        }
        Ok(updated)
    }

    fn apply_assignments(
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

            let value = Self::evaluate_expr(expr, &tuple.data, schema, catalog)?;

            Self::validate_type(&schema.columns[idx].data_type, &value, col_name)?;
            tuple.data[idx] = value;
        }
        Ok(())
    }

    fn apply_assignments_with_tuples(
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

            let value = Self::evaluate_expr_with_tuples(
                expr,
                &tuple.data,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;

            Self::validate_type(&schema.columns[idx].data_type, &value, col_name)?;
            tuple.data[idx] = value;
        }
        Ok(())
    }

    fn evaluate_expr(
        expr: &Expr,
        tuple_data: &[Value],
        schema: &TableSchema,
        catalog: &Catalog,
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
            Expr::UnaryOp { op, expr } => {
                let val = Self::evaluate_expr(expr, tuple_data, schema, catalog)?;
                eval_unary_op(op, &val)
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::evaluate_expr(left, tuple_data, schema, catalog)?;
                let r = Self::evaluate_expr(right, tuple_data, schema, catalog)?;
                eval_binary_op(&l, op, &r)
            }
            Expr::IsNull(expr) => {
                let val = Self::evaluate_expr(expr, tuple_data, schema, catalog)?;
                Ok(Value::Bool(matches!(val, Value::Null)))
            }
            Expr::IsNotNull(expr) => {
                let val = Self::evaluate_expr(expr, tuple_data, schema, catalog)?;
                Ok(Value::Bool(!matches!(val, Value::Null)))
            }
            Expr::Case { conditions, else_expr } => {
                for (when_expr, then_expr) in conditions {
                    let when_val = Self::evaluate_expr(when_expr, tuple_data, schema, catalog)?;
                    if when_val == Value::Bool(true) {
                        return Self::evaluate_expr(then_expr, tuple_data, schema, catalog);
                    }
                }
                if let Some(else_e) = else_expr {
                    Self::evaluate_expr(else_e, tuple_data, schema, catalog)
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Subquery(subquery) => {
                match SelectExecutor::eval_scalar_subquery(catalog, subquery) {
                    Ok(value) => Ok(value),
                    Err(_) => Ok(Value::Null),
                }
            }
            _ => Err(format!("Unsupported expression type in UPDATE SET: {:?}", expr)),
        }
    }

    fn evaluate_expr_with_tuples(
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
            Expr::UnaryOp { op, expr } => {
                let val = Self::evaluate_expr_with_tuples(
                    expr,
                    tuple_data,
                    schema,
                    catalog,
                    subquery_tuples,
                    snapshot,
                )?;
                eval_unary_op(op, &val)
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::evaluate_expr_with_tuples(
                    left,
                    tuple_data,
                    schema,
                    catalog,
                    subquery_tuples,
                    snapshot,
                )?;
                let r = Self::evaluate_expr_with_tuples(
                    right,
                    tuple_data,
                    schema,
                    catalog,
                    subquery_tuples,
                    snapshot,
                )?;
                eval_binary_op(&l, op, &r)
            }
            Expr::IsNull(expr) => {
                let val = Self::evaluate_expr_with_tuples(
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
                let val = Self::evaluate_expr_with_tuples(
                    expr,
                    tuple_data,
                    schema,
                    catalog,
                    subquery_tuples,
                    snapshot,
                )?;
                Ok(Value::Bool(!matches!(val, Value::Null)))
            }
            Expr::Case { conditions, else_expr } => {
                for (when_expr, then_expr) in conditions {
                    let when_val = Self::evaluate_expr_with_tuples(
                        when_expr,
                        tuple_data,
                        schema,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    )?;
                    if when_val == Value::Bool(true) {
                        return Self::evaluate_expr_with_tuples(
                            then_expr,
                            tuple_data,
                            schema,
                            catalog,
                            subquery_tuples,
                            snapshot,
                        );
                    }
                }
                if let Some(else_e) = else_expr {
                    Self::evaluate_expr_with_tuples(
                        else_e,
                        tuple_data,
                        schema,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    )
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Subquery(subquery) => {
                // Use the provided tuples and outer query's snapshot to evaluate the subquery
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
            _ => Err(format!("Unsupported expression type in UPDATE SET: {:?}", expr)),
        }
    }

    fn validate_type(data_type: &DataType, value: &Value, col_name: &str) -> Result<(), String> {
        match (data_type, value) {
            (DataType::Int, Value::Int(_))
            | (DataType::Float, Value::Float(_))
            | (DataType::Float, Value::Int(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Varchar(_), Value::Text(_))
            | (DataType::Boolean, Value::Bool(_)) => Ok(()),
            _ => Err(format!("Type mismatch for column '{}'", col_name)),
        }
    }

    pub fn delete(
        tuples: &mut [Tuple],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        xid: u64,
    ) -> Result<usize, String> {
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            tuple.header.delete(xid);
            deleted += 1;
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::TableSchema;
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};
    use crate::transaction::{Snapshot, TransactionManager, TupleHeader};
    use std::collections::HashMap;

    // Mock PredicateEvaluator for testing purposes
    // In a real scenario, you might have a more sophisticated mock or use test data that
    // always makes PredicateEvaluator::evaluate return true for simplicity if the focus
    // is not on predicate evaluation itself.
    // Here, we'll implement a basic mock that only evaluates simple equality expressions.
    struct MockPredicateEvaluator;

    impl MockPredicateEvaluator {
        fn eval_expr(
            expr: &Expr,
            tuple_data: &[Value],
            schema: &TableSchema,
        ) -> Result<Value, String> {
            match expr {
                Expr::Column(name) => {
                    let lookup_name = if let Some(dot_pos) = name.find('.') {
                        &name[dot_pos + 1..]
                    } else {
                        name
                    };
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| &c.name == lookup_name)
                        .ok_or_else(|| format!("Column '{}' not found", name))?;
                    Ok(tuple_data[idx].clone())
                }
                Expr::Number(n) => Ok(Value::Int(*n)),
                Expr::String(s) => Ok(Value::Text(s.clone())),
                Expr::Null => Ok(Value::Null),
                Expr::BinaryOp { left, op, right } => {
                    let l = Self::eval_expr(left, tuple_data, schema)?;
                    let r = Self::eval_expr(right, tuple_data, schema)?;
                    crate::executor::expr_evaluator::eval_binary_op(&l, op, &r)
                }
                Expr::UnaryOp { op, expr } => {
                    let val = Self::eval_expr(expr, tuple_data, schema)?;
                    crate::executor::expr_evaluator::eval_unary_op(op, &val)
                }
                _ => Err(format!("Unsupported expression in mock predicate: {:?}", expr)),
            }
        }

        fn evaluate(
            expr: &Expr,
            tuple_data: &[Value],
            schema: &TableSchema,
        ) -> Result<bool, String> {
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    let left_val = Self::eval_expr(left, tuple_data, schema)?;
                    let right_val = Self::eval_expr(right, tuple_data, schema)?;
                    match op {
                        BinaryOperator::Equals => Ok(left_val == right_val),
                        BinaryOperator::NotEquals => Ok(left_val != right_val),
                        BinaryOperator::GreaterThan => {
                            let result = crate::executor::expr_evaluator::eval_binary_op(
                                &left_val, op, &right_val,
                            )?;
                            match result {
                                Value::Bool(b) => Ok(b),
                                _ => Err("Comparison must return bool".to_string()),
                            }
                        }
                        BinaryOperator::LessThan => {
                            let result = crate::executor::expr_evaluator::eval_binary_op(
                                &left_val, op, &right_val,
                            )?;
                            match result {
                                Value::Bool(b) => Ok(b),
                                _ => Err("Comparison must return bool".to_string()),
                            }
                        }
                        _ => Err(format!("Unsupported operator in mock predicate: {:?}", op)),
                    }
                }
                _ => Err("Unsupported expression in mock predicate".to_string()),
            }
        }
    }

    fn update_with_mock_evaluator(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        catalog: &Catalog,
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !MockPredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            UpdateDeleteExecutor::apply_assignments(tuple, assignments, schema, catalog)?;
            updated += 1;
        }
        Ok(updated)
    }

    fn delete_with_mock_evaluator(
        tuples: &mut [Tuple],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        xid: u64,
    ) -> Result<usize, String> {
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !MockPredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            tuple.header.delete(xid);
            deleted += 1;
        }
        Ok(deleted)
    }

    // Helper to create a test schema
    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("age".to_string(), DataType::Int),
            ],
        )
    }

    // Helper to create a test tuple
    fn create_test_tuple(xmin: u64, id: i64, name: &str, age: i64) -> Tuple {
        let mut tuple =
            Tuple { header: TupleHeader::new(xmin), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(id));
        tuple.add_value("name".to_string(), Value::Text(name.to_string()));
        tuple.add_value("age".to_string(), Value::Int(age));
        tuple
    }

    // --- validate_type tests ---
    #[test]
    fn test_validate_type_success() {
        assert!(
            UpdateDeleteExecutor::validate_type(&DataType::Int, &Value::Int(10), "col").is_ok()
        );
        assert!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Text,
                &Value::Text("hello".to_string()),
                "col"
            )
            .is_ok()
        );
        assert!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Varchar(10),
                &Value::Text("short".to_string()),
                "col"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_validate_type_mismatch() {
        assert!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Int,
                &Value::Text("hello".to_string()),
                "col"
            )
            .is_err()
        );
        assert_eq!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Int,
                &Value::Text("hello".to_string()),
                "col"
            )
            .unwrap_err(),
            "Type mismatch for column 'col'"
        );

        assert!(
            UpdateDeleteExecutor::validate_type(&DataType::Text, &Value::Int(10), "col").is_err()
        );
    }

    // --- apply_assignments tests ---
    #[test]
    fn test_apply_assignments_success() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![
            ("name".to_string(), Expr::String("Bob".to_string())),
            ("age".to_string(), Expr::Number(31)),
        ];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("name"), Some(Value::Text("Bob".to_string())));
        assert_eq!(tuple.get_value("age"), Some(Value::Int(31)));
    }

    #[test]
    fn test_apply_assignments_column_not_found() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("non_existent".to_string(), Expr::Number(100))];

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Column 'non_existent' not found");
    }

    #[test]
    fn test_apply_assignments_type_mismatch() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("name".to_string(), Expr::Number(123))]; // name is TEXT

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Type mismatch for column 'name'");
    }

    #[test]
    fn test_apply_assignments_invalid_expression() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("age".to_string(), Expr::Star)];

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported expression"));
    }

    // --- arithmetic expression tests ---
    #[test]
    fn test_apply_assignments_column_reference() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("age".to_string(), Expr::Column("id".to_string()))];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(1)));
    }

    #[test]
    fn test_apply_assignments_column_qualified() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::QualifiedColumn { table: "users".to_string(), column: "id".to_string() },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(1)));
    }

    #[test]
    fn test_apply_assignments_expr_column_not_found() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("age".to_string(), Expr::Column("missing".to_string()))];

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column 'missing' not found"));
    }

    #[test]
    fn test_apply_assignments_arithmetic_add() {
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

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(31)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_subtract() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Subtract,
                right: Box::new(Expr::Number(5)),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(25)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_multiply() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(2)),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(60)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_divide() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Divide,
                right: Box::new(Expr::Number(3)),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(10)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_modulo() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Modulo,
                right: Box::new(Expr::Number(7)),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(2)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_division_by_zero() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Divide,
                right: Box::new(Expr::Number(0)),
            },
        )];

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Division by zero"));
    }

    #[test]
    fn test_apply_assignments_modulo_by_zero() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Modulo,
                right: Box::new(Expr::Number(0)),
            },
        )];

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Division by zero"));
    }

    #[test]
    fn test_apply_assignments_nested_arithmetic() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Number(2)),
                }),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Column("id".to_string())),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(61)));
    }

    #[test]
    fn test_apply_assignments_deeply_nested_arithmetic() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 10);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Add,
                        right: Box::new(Expr::Number(1)),
                    }),
                }),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(3)),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(23)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_with_literals() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 0);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Number(10)),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(3)),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(30)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_between_two_columns() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 10);
        let assignments = vec![(
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Column("age".to_string())),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(10)));
    }

    #[test]
    fn test_apply_assignments_unary_minus() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![(
            "age".to_string(),
            Expr::UnaryOp {
                op: crate::parser::ast::UnaryOperator::Minus,
                expr: Box::new(Expr::Column("age".to_string())),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("age"), Some(Value::Int(-30)));
    }

    #[test]
    fn test_apply_assignments_is_null() {
        let schema = TableSchema::new(
            "data".to_string(),
            vec![
                ColumnDef::new("flag".to_string(), DataType::Boolean),
                ColumnDef::new("val".to_string(), DataType::Int),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("flag".to_string(), Value::Bool(false));
        tuple.add_value("val".to_string(), Value::Null);

        let assignments =
            vec![("flag".to_string(), Expr::IsNull(Box::new(Expr::Column("val".to_string()))))];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("flag"), Some(Value::Bool(true)));
    }

    #[test]
    fn test_apply_assignments_is_not_null() {
        let schema = TableSchema::new(
            "data".to_string(),
            vec![
                ColumnDef::new("flag".to_string(), DataType::Boolean),
                ColumnDef::new("val".to_string(), DataType::Int),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("flag".to_string(), Value::Bool(false));
        tuple.add_value("val".to_string(), Value::Int(42));

        let assignments =
            vec![("flag".to_string(), Expr::IsNotNull(Box::new(Expr::Column("val".to_string()))))];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("flag"), Some(Value::Bool(true)));
    }

    #[test]
    fn test_apply_assignments_arithmetic_result_type_mismatch() {
        let schema = TableSchema::new(
            "data".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("name".to_string(), Value::Text("hello".to_string()));

        let assignments = vec![(
            "name".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Number(1)),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(2)),
            },
        )];

        let result = UpdateDeleteExecutor::apply_assignments(
            &mut tuple,
            &assignments,
            &schema,
            &Catalog::new(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Type mismatch"));
    }

    #[test]
    fn test_apply_assignments_multiple_arithmetic_assignments() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![
            (
                "id".to_string(),
                Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Add,
                    right: Box::new(Expr::Number(10)),
                },
            ),
            (
                "age".to_string(),
                Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Number(2)),
                },
            ),
        ];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("id"), Some(Value::Int(11)));
        assert_eq!(tuple.get_value("age"), Some(Value::Int(60)));
    }

    // --- CASE expression tests ---
    #[test]
    fn test_apply_assignments_case_expression() {
        let schema = TableSchema::new(
            "accounts".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("balance".to_string(), DataType::Int),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("balance".to_string(), Value::Int(150));

        let assignments = vec![(
            "balance".to_string(),
            Expr::Case {
                conditions: vec![(
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("balance".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Number(100)),
                    },
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("balance".to_string())),
                        op: BinaryOperator::Subtract,
                        right: Box::new(Expr::Number(10)),
                    },
                )],
                else_expr: Some(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("balance".to_string())),
                    op: BinaryOperator::Add,
                    right: Box::new(Expr::Number(10)),
                })),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("balance"), Some(Value::Int(140)));
    }

    #[test]
    fn test_apply_assignments_case_expression_else_branch() {
        let schema = TableSchema::new(
            "accounts".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("balance".to_string(), DataType::Int),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("balance".to_string(), Value::Int(50));

        let assignments = vec![(
            "balance".to_string(),
            Expr::Case {
                conditions: vec![(
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("balance".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Number(100)),
                    },
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("balance".to_string())),
                        op: BinaryOperator::Subtract,
                        right: Box::new(Expr::Number(10)),
                    },
                )],
                else_expr: Some(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("balance".to_string())),
                    op: BinaryOperator::Add,
                    right: Box::new(Expr::Number(10)),
                })),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("balance"), Some(Value::Int(60)));
    }

    #[test]
    fn test_apply_assignments_case_expression_no_else() {
        let schema = TableSchema::new(
            "data".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("value".to_string(), Value::Int(50));

        let assignments = vec![(
            "value".to_string(),
            Expr::Case {
                conditions: vec![(
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("value".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Number(100)),
                    },
                    Expr::Number(999),
                )],
                else_expr: Some(Box::new(Expr::Number(0))),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("value"), Some(Value::Int(0)));
    }

    #[test]
    fn test_apply_assignments_case_expression_multiple_conditions() {
        let schema = TableSchema::new(
            "grade".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("score".to_string(), DataType::Int),
                ColumnDef::new("grade".to_string(), DataType::Text),
            ],
        );
        let mut tuple =
            Tuple { header: TupleHeader::new(1), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("score".to_string(), Value::Int(85));
        tuple.add_value("grade".to_string(), Value::Text("B".to_string()));

        let assignments = vec![(
            "grade".to_string(),
            Expr::Case {
                conditions: vec![
                    (
                        Expr::BinaryOp {
                            left: Box::new(Expr::Column("score".to_string())),
                            op: BinaryOperator::GreaterThanOrEqual,
                            right: Box::new(Expr::Number(90)),
                        },
                        Expr::String("A".to_string()),
                    ),
                    (
                        Expr::BinaryOp {
                            left: Box::new(Expr::Column("score".to_string())),
                            op: BinaryOperator::GreaterThanOrEqual,
                            right: Box::new(Expr::Number(80)),
                        },
                        Expr::String("B".to_string()),
                    ),
                    (
                        Expr::BinaryOp {
                            left: Box::new(Expr::Column("score".to_string())),
                            op: BinaryOperator::GreaterThanOrEqual,
                            right: Box::new(Expr::Number(70)),
                        },
                        Expr::String("C".to_string()),
                    ),
                ],
                else_expr: Some(Box::new(Expr::String("F".to_string()))),
            },
        )];

        assert!(
            UpdateDeleteExecutor::apply_assignments(
                &mut tuple,
                &assignments,
                &schema,
                &Catalog::new()
            )
            .is_ok()
        );
        assert_eq!(tuple.get_value("grade"), Some(Value::Text("B".to_string())));
    }

    // --- update tests ---
    #[test]
    fn test_update_single_tuple() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        txn_mgr.commit(txn.xid).unwrap();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let snapshot = Snapshot::new(txn.xid, txn.xid + 1, vec![]); // Mock snapshot

        let catalog = Catalog::new();
        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &None, // No WHERE clause
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 1);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(31)));
    }

    #[test]
    fn test_update_multiple_tuples_with_where() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![
            create_test_tuple(xid_creator, 1, "Alice", 30),
            create_test_tuple(xid_creator, 2, "Bob", 25),
            create_test_tuple(xid_creator, 3, "Alice", 35),
        ];
        let assignments = vec![("age".to_string(), Expr::Number(40))];
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Alice".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let catalog = Catalog::new();

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 2);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(40)));
        assert_eq!(tuples[1].get_value("age"), Some(Value::Int(25))); // Bob not updated
        assert_eq!(tuples[2].get_value("age"), Some(Value::Int(40)));
    }

    #[test]
    fn test_update_no_matching_tuples() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![create_test_tuple(xid_creator, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Bob".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let catalog = Catalog::new();

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 0);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(30))); // Not updated
    }

    #[test]
    fn test_update_tuple_not_visible() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let snapshot = Snapshot::new(0, txn.xid, vec![txn.xid]); // Snapshot before txn commits
        let catalog = Catalog::new();

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &None,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 0);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(30))); // Not updated
    }

    // --- delete tests ---
    #[test]
    fn test_delete_single_tuple() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid = txn_mgr.begin().xid;
        txn_mgr.commit(xid).unwrap(); // Commit the transaction that created the tuple
        let mut tuples = vec![create_test_tuple(xid, 1, "Alice", 30)];
        let snapshot = Snapshot::new(xid, xid + 1, vec![]);

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &None, // No WHERE clause
            &schema,
            &snapshot,
            &txn_mgr,
            xid + 1, // XID for the deleting transaction
        )
        .unwrap();

        assert_eq!(deleted_count, 1);
        // Verify that the tuple is marked as deleted (xmax is set)
        assert_ne!(tuples[0].header.xmax, 0);
    }

    #[test]
    fn test_delete_multiple_tuples_with_where() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![
            create_test_tuple(xid_creator, 1, "Alice", 30),
            create_test_tuple(xid_creator, 2, "Bob", 25),
            create_test_tuple(xid_creator, 3, "Alice", 35),
        ];

        // Make tuples visible
        for tuple in tuples.iter_mut() {
            tuple.header.xmin = xid_creator;
        }

        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Alice".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let xid_deleter = txn_mgr.begin().xid;

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            xid_deleter,
        )
        .unwrap();

        assert_eq!(deleted_count, 2);
        assert_ne!(tuples[0].header.xmax, 0); // Alice 1 deleted
        assert_eq!(tuples[1].header.xmax, 0); // Bob not deleted
        assert_ne!(tuples[2].header.xmax, 0); // Alice 2 deleted
    }

    #[test]
    fn test_delete_no_matching_tuples() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![create_test_tuple(xid_creator, 1, "Alice", 30)];

        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Bob".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let xid_deleter = txn_mgr.begin().xid;

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            xid_deleter,
        )
        .unwrap();

        assert_eq!(deleted_count, 0);
        assert_eq!(tuples[0].header.xmax, 0); // Not deleted
    }

    #[test]
    fn test_delete_tuple_not_visible() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        // Tuple's xmin is txn.xid, but its transaction is not committed.
        // So a snapshot with xmax txn.xid (default) won't see it.
        let snapshot = Snapshot::new(0, txn.xid, vec![txn.xid]); // Before tuple's creation txn was committed
        let xid_deleter = txn_mgr.begin().xid;

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &None,
            &schema,
            &snapshot,
            &txn_mgr,
            xid_deleter,
        )
        .unwrap();

        assert_eq!(deleted_count, 0);
        assert_eq!(tuples[0].header.xmax, 0); // Not deleted
    }
}
