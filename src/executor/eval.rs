//! Expression evaluation for the Volcano executor model
//!
//! This module provides expression evaluation capabilities for the query executor.
//! It evaluates SQL expressions against tuples (rows) of data.
//!
//! # Architecture
//!
//! Expression evaluation is split into multiple modules for clarity:
//! - `eval.rs` - Main entry point and expression matching
//! - `eval_binary.rs` - Binary operation evaluation
//! - `eval_builtins.rs` - Builtin SQL function evaluation
//! - `eval_helpers.rs` - Shared helper functions

use super::eval_binary;
use super::eval_builtins;
use super::eval_helpers;

pub use eval_binary::eval_binary_op;
pub use eval_builtins::{eval_builtin_function, eval_unary_op};

use super::operators::executor::{ExecutorError, Tuple};
use crate::catalog::select_executor::SelectExecutor;
use crate::catalog::tuple::Tuple as CatalogTuple;
use crate::catalog::{Catalog, EnumTypeDef, Value};
use crate::parser::ast::{BinaryOperator, Expr, SelectStmt};
use crate::transaction::Snapshot;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub struct Eval;

impl Eval {
    /// Evaluate an expression given a tuple (HashMap of column values)
    pub fn eval_expr(expr: &Expr, tuple: &Tuple) -> Result<Value, ExecutorError> {
        Self::eval_expr_with_catalog(expr, tuple, None, None, None)
    }

    /// Evaluate an expression with optional catalog for subqueries
    ///
    /// Arguments:
    /// * `expr` - The expression to evaluate
    /// * `tuple` - The tuple (row) to evaluate against
    /// * `catalog` - Optional catalog for function resolution
    /// * `subquery_tuples` - Optional pre-fetched tuples for subquery evaluation
    /// * `snapshot` - Optional snapshot for MVCC visibility
    pub fn eval_expr_with_catalog(
        expr: &Expr,
        tuple: &Tuple,
        catalog: Option<&Catalog>,
        subquery_tuples: Option<&[Tuple]>,
        snapshot: Option<&Snapshot>,
    ) -> Result<Value, ExecutorError> {
        Self::eval_expr_internal(expr, tuple, catalog, subquery_tuples, snapshot)
    }

    /// Internal expression evaluation with full parameters
    fn eval_expr_internal(
        expr: &Expr,
        tuple: &Tuple,
        catalog: Option<&Catalog>,
        subquery_tuples: Option<&[Tuple]>,
        snapshot: Option<&Snapshot>,
    ) -> Result<Value, ExecutorError> {
        match expr {
            Expr::Column(name) => Self::eval_column(tuple, name),

            Expr::QualifiedColumn { table, column } => {
                let qualified_name = format!("{}.{}", table, column);
                tuple
                    .get(&qualified_name)
                    .cloned()
                    .or_else(|| tuple.get(column).cloned())
                    .ok_or(ExecutorError::ColumnNotFound(qualified_name))
            }

            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Null => Ok(Value::Null),
            Expr::Star => Err(ExecutorError::UnsupportedExpression(
                "* not allowed in this context".to_string(),
            )),

            Expr::Tuple(exprs) => {
                if exprs.is_empty() {
                    Err(ExecutorError::InternalError("Empty tuple".to_string()))
                } else {
                    Self::eval_expr_internal(&exprs[0], tuple, catalog, subquery_tuples, snapshot)
                }
            }

            Expr::BinaryOp { left, op, right } => Self::eval_binary_op_expr(
                left.as_ref(),
                op,
                right.as_ref(),
                tuple,
                catalog,
                subquery_tuples,
                snapshot,
            ),

            Expr::UnaryOp { op, expr } => {
                let val =
                    Self::eval_expr_internal(expr, tuple, catalog, subquery_tuples, snapshot)?;
                eval_builtins::eval_unary_op(op, &val)
            }

            Expr::IsNull(inner) => {
                let val =
                    Self::eval_expr_internal(inner, tuple, catalog, subquery_tuples, snapshot)?;
                Ok(Value::Bool(matches!(val, Value::Null)))
            }

            Expr::IsNotNull(inner) => {
                let val =
                    Self::eval_expr_internal(inner, tuple, catalog, subquery_tuples, snapshot)?;
                Ok(Value::Bool(!matches!(val, Value::Null)))
            }

            Expr::FunctionCall { name, args } => {
                let mut evaluated_args = Vec::new();
                for arg in args {
                    evaluated_args.push(Self::eval_expr_internal(
                        arg,
                        tuple,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    )?);
                }
                Self::eval_function_call(name, evaluated_args, catalog)
            }

            Expr::Aggregate { func: _, arg } => {
                if matches!(arg.as_ref(), Expr::Star) {
                    Ok(Value::Int(1))
                } else {
                    Self::eval_expr_internal(arg, tuple, catalog, subquery_tuples, snapshot)
                }
            }

            Expr::Case { conditions, else_expr } => {
                for (condition, result) in conditions {
                    let cond_val = Self::eval_expr_internal(
                        condition,
                        tuple,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    )?;
                    if let Value::Bool(true) = cond_val {
                        return Self::eval_expr_internal(
                            result,
                            tuple,
                            catalog,
                            subquery_tuples,
                            snapshot,
                        );
                    }
                }
                if let Some(else_expr) = else_expr {
                    Self::eval_expr_internal(else_expr, tuple, catalog, subquery_tuples, snapshot)
                } else {
                    Ok(Value::Null)
                }
            }

            Expr::Alias { expr, alias: _ } => {
                Self::eval_expr_internal(expr, tuple, catalog, subquery_tuples, snapshot)
            }

            Expr::Parameter(_) => Err(ExecutorError::UnsupportedExpression(
                "Parameters not supported in this context".to_string(),
            )),

            Expr::List(_) => Err(ExecutorError::UnsupportedExpression(
                "List not supported in this context".to_string(),
            )),

            Expr::Array(arr) => {
                let mut values = Vec::new();
                for elem in arr {
                    values.push(Self::eval_expr_internal(
                        elem,
                        tuple,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    )?);
                }
                Ok(Value::Array(values))
            }

            Expr::Range { .. } => Err(ExecutorError::UnsupportedExpression(
                "Range literals not supported in this context".to_string(),
            )),

            Expr::Row(exprs) => {
                let mut values = Vec::new();
                for expr in exprs {
                    values.push(Self::eval_expr_internal(
                        expr,
                        tuple,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    )?);
                }
                Ok(Value::Text(format!(
                    "ROW({})",
                    values.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(", ")
                )))
            }

            Expr::Subquery(stmt) => {
                if let Some(cat) = catalog {
                    Self::eval_scalar_subquery(cat, stmt)
                } else {
                    Err(ExecutorError::UnsupportedExpression(
                        "Subqueries require catalog".to_string(),
                    ))
                }
            }

            Expr::Window { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions not supported in this context".to_string(),
            )),

            Expr::CustomAggregate { .. } => Err(ExecutorError::UnsupportedExpression(
                "Custom aggregates must be executed via HashAggExecutor".to_string(),
            )),
        }
    }

    /// Evaluate a column reference
    fn eval_column(tuple: &Tuple, name: &str) -> Result<Value, ExecutorError> {
        if name.contains('.') {
            tuple.get(name).cloned().ok_or_else(|| ExecutorError::ColumnNotFound(name.to_string()))
        } else {
            if let Some(value) = tuple.get(name) {
                return Ok(value.clone());
            }
            let matches: Vec<_> = tuple
                .iter()
                .filter(|(k, _)| k.ends_with(&format!(".{}", name)) || *k == name)
                .collect();

            if matches.is_empty() {
                Err(ExecutorError::ColumnNotFound(name.to_string()))
            } else if matches.len() > 1 {
                Err(ExecutorError::AmbiguousColumn(format!(
                    "Column '{}' is ambiguous. Found: {:?}",
                    name,
                    matches.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>()
                )))
            } else {
                Ok(matches[0].1.clone())
            }
        }
    }

    /// Evaluate a binary operation expression
    fn eval_binary_op_expr(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        tuple: &Tuple,
        catalog: Option<&Catalog>,
        subquery_tuples: Option<&[Tuple]>,
        snapshot: Option<&Snapshot>,
    ) -> Result<Value, ExecutorError> {
        let enum_types = catalog
            .map(|c| c.enum_types.clone())
            .unwrap_or_else(|| Arc::new(RwLock::new(HashMap::new())));

        if *op == BinaryOperator::In {
            if let Expr::List(values) = right {
                let left_val =
                    Self::eval_expr_internal(left, tuple, catalog, subquery_tuples, snapshot)?;
                let mut found = false;
                for val_expr in values {
                    if let Ok(val) = Self::eval_expr_internal(
                        val_expr,
                        tuple,
                        catalog,
                        subquery_tuples,
                        snapshot,
                    ) {
                        if eval_helpers::values_equal_with_enum_support(
                            &left_val,
                            &val,
                            &enum_types,
                        )? {
                            found = true;
                            break;
                        }
                    }
                }
                return Ok(Value::Bool(found));
            }

            if let Expr::Subquery(stmt) = right {
                if let Some(cat) = catalog {
                    let left_val = Self::eval_expr_internal(
                        left,
                        tuple,
                        Some(cat),
                        subquery_tuples,
                        snapshot,
                    )?;
                    let catalog_arc = Arc::new(cat.clone());
                    let result = Catalog::select_with_catalog(
                        &catalog_arc,
                        &stmt.from,
                        stmt.distinct,
                        stmt.columns.clone(),
                        stmt.where_clause.clone(),
                        stmt.group_by.clone(),
                        stmt.having.clone(),
                        stmt.order_by.clone(),
                        stmt.limit,
                        stmt.offset,
                    );
                    match result {
                        Ok(rows) => {
                            let found = rows.iter().any(|row| row.len() == 1 && row[0] == left_val);
                            return Ok(Value::Bool(found));
                        }
                        Err(e) => {
                            return Err(ExecutorError::InternalError(format!(
                                "IN subquery failed: {}",
                                e
                            )));
                        }
                    }
                } else {
                    return Err(ExecutorError::UnsupportedExpression(
                        "IN subqueries require catalog".to_string(),
                    ));
                }
            }
        }

        let left_val = Self::eval_expr_internal(left, tuple, catalog, subquery_tuples, snapshot)?;
        let right_val = Self::eval_expr_internal(right, tuple, catalog, subquery_tuples, snapshot)?;
        eval_binary::eval_binary_op(&left_val, op, &right_val, &enum_types)
    }

    /// Evaluate a scalar subquery (returns single value)
    fn eval_scalar_subquery(catalog: &Catalog, stmt: &SelectStmt) -> Result<Value, ExecutorError> {
        let catalog_arc = Arc::new(catalog.clone());
        let result = Catalog::select_with_catalog(
            &catalog_arc,
            &stmt.from,
            stmt.distinct,
            stmt.columns.clone(),
            stmt.where_clause.clone(),
            stmt.group_by.clone(),
            stmt.having.clone(),
            stmt.order_by.clone(),
            stmt.limit,
            stmt.offset,
        );

        match result {
            Ok(rows) => {
                if rows.is_empty() {
                    Ok(Value::Null)
                } else if rows.len() == 1 && rows[0].len() == 1 {
                    Ok(rows[0][0].clone())
                } else if rows.len() == 1 {
                    Ok(rows[0][0].clone())
                } else {
                    Ok(rows[0][0].clone())
                }
            }
            Err(e) => {
                Err(ExecutorError::InternalError(format!("Subquery execution failed: {}", e)))
            }
        }
    }

    /// Evaluate a function call (builtin or user-defined SQL function)
    pub fn eval_function_call(
        name: &str,
        args: Vec<Value>,
        catalog: Option<&Catalog>,
    ) -> Result<Value, ExecutorError> {
        if let Some(result) = eval_builtins::eval_builtin_function(name, &args) {
            return result;
        }

        if let Some(catalog) = catalog {
            if let Some(func) =
                catalog.get_function(name, &args.iter().map(|v| v.type_name()).collect::<Vec<_>>())
            {
                if func.language == crate::catalog::FunctionLanguage::Sql {
                    return Self::eval_sql_function(&func, args, catalog);
                }
            }
        }

        Err(ExecutorError::FunctionNotFound(format!("Function '{}' not found", name)))
    }

    /// Evaluate a user-defined SQL function
    fn eval_sql_function(
        func: &crate::catalog::Function,
        args: Vec<Value>,
        catalog: &Catalog,
    ) -> Result<Value, ExecutorError> {
        let substituted_body = Self::substitute_params_in_body(&func.body, &args);

        let mut parser = crate::parser::Parser::new(&substituted_body)
            .map_err(|e| ExecutorError::InternalError(format!("Failed to create parser: {}", e)))?;

        let stmt = parser.parse().map_err(|e| {
            ExecutorError::InternalError(format!("Failed to parse function body: {}", e))
        })?;

        match stmt {
            crate::parser::ast::Statement::Select(select) => {
                let catalog_arc = Arc::new(catalog.clone());
                let planner =
                    crate::planner::planner::Planner::new_with_catalog(catalog_arc.clone());
                let mut plan = planner.plan(&select).map_err(|e| {
                    ExecutorError::InternalError(format!("Failed to plan function body: {}", e))
                })?;

                let mut tuples = Vec::new();
                while let Some(tuple) = plan.next()? {
                    tuples.push(tuple);
                }

                if tuples.is_empty() {
                    return Ok(Value::Null);
                }

                let first_tuple = &tuples[0];
                if first_tuple.is_empty() {
                    return Ok(Value::Null);
                }

                Ok(first_tuple.values().next().cloned().unwrap_or(Value::Null))
            }
            _ => Err(ExecutorError::InternalError(format!(
                "SQL function body must be a SELECT statement"
            ))),
        }
    }

    /// Substitute parameters ($1, $2, etc.) in function body with actual values
    fn substitute_params_in_body(body: &str, params: &[Value]) -> String {
        let mut result = body.to_string();
        for (i, param) in params.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            let value_str = eval_helpers::value_to_sql_string(param);
            result = result.replace(&placeholder, &value_str);
        }
        result
    }

    /// Evaluate a function call (2-arg version for backwards compatibility)
    pub fn eval_function(name: &str, args: Vec<Value>) -> Result<Value, ExecutorError> {
        Self::eval_function_call(name, args, None)
    }

    /// Evaluate a function call (3-arg version with catalog)
    pub fn eval_function_with_catalog(
        name: &str,
        args: Vec<Value>,
        catalog: Option<&Catalog>,
    ) -> Result<Value, ExecutorError> {
        Self::eval_function_call(name, args, catalog)
    }
}

// ============================================================================
// Re-exports for backwards compatibility
// ============================================================================

/// Evaluate a binary operation (re-exported for compatibility)
pub fn eval_binary_operation(
    left: &Value,
    op: &BinaryOperator,
    right: &Value,
    enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
) -> Result<Value, ExecutorError> {
    eval_binary::eval_binary_op(left, op, right, enum_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Range;
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
    use std::collections::HashMap;

    fn create_test_tuple() -> Tuple {
        let mut tuple = Tuple::new();
        tuple.insert("a".to_string(), Value::Int(10));
        tuple.insert("b".to_string(), Value::Text("hello".to_string()));
        tuple.insert("c".to_string(), Value::Bool(true));
        tuple.insert("d".to_string(), Value::Null);
        tuple
    }

    fn empty_enum_types() -> Arc<RwLock<HashMap<String, EnumTypeDef>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[test]
    fn test_eval_literals() {
        let tuple = create_test_tuple();
        assert_eq!(Eval::eval_expr(&Expr::Number(123), &tuple).unwrap(), Value::Int(123));
        assert_eq!(
            Eval::eval_expr(&Expr::String("test".to_string()), &tuple).unwrap(),
            Value::Text("test".to_string())
        );
    }

    #[test]
    fn test_eval_column() {
        let tuple = create_test_tuple();
        assert_eq!(
            Eval::eval_expr(&Expr::Column("a".to_string()), &tuple).unwrap(),
            Value::Int(10)
        );
        assert_eq!(
            Eval::eval_expr(&Expr::Column("b".to_string()), &tuple).unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_eval_binary_op_add() {
        let tuple = create_test_tuple();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(Eval::eval_expr(&expr, &tuple).unwrap(), Value::Int(15));
    }

    #[test]
    fn test_eval_binary_op_equals() {
        let tuple = create_test_tuple();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(10)),
        };
        assert_eq!(Eval::eval_expr(&expr, &tuple).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_unary_op_not() {
        let tuple = create_test_tuple();
        let expr =
            Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(Expr::Column("c".to_string())) };
        assert_eq!(Eval::eval_expr(&expr, &tuple).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_is_null() {
        let tuple = create_test_tuple();
        let expr = Expr::IsNull(Box::new(Expr::Column("d".to_string())));
        assert_eq!(Eval::eval_expr(&expr, &tuple).unwrap(), Value::Bool(true));

        let expr_not_null = Expr::IsNotNull(Box::new(Expr::Column("a".to_string())));
        assert_eq!(Eval::eval_expr(&expr_not_null, &tuple).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_case_expression() {
        let tuple = create_test_tuple();
        let expr = Expr::Case {
            conditions: vec![
                (
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("a".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Number(5)),
                    },
                    Expr::String("five".to_string()),
                ),
                (
                    Expr::BinaryOp {
                        left: Box::new(Expr::Column("a".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Number(10)),
                    },
                    Expr::String("ten".to_string()),
                ),
            ],
            else_expr: Some(Box::new(Expr::String("other".to_string()))),
        };
        assert_eq!(Eval::eval_expr(&expr, &tuple).unwrap(), Value::Text("ten".to_string()));
    }

    #[test]
    fn test_in_operator() {
        let tuple = create_test_tuple();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![Expr::Number(5), Expr::Number(10), Expr::Number(15)])),
        };
        assert_eq!(Eval::eval_expr(&expr, &tuple).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_concat_function() {
        let result = eval_builtins::eval_builtin_function(
            "CONCAT",
            &[Value::Text("hello".to_string()), Value::Text("world".to_string())],
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_coalesce_returns_first_non_null() {
        let result = eval_builtins::eval_builtin_function(
            "COALESCE",
            &[Value::Null, Value::Null, Value::Text("found".to_string())],
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, Value::Text("found".to_string()));
    }

    #[test]
    fn test_nullif_equal_returns_null() {
        let result = eval_builtins::eval_builtin_function(
            "NULLIF",
            &[Value::Text("same".to_string()), Value::Text("same".to_string())],
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_range_adjacent_true() {
        let range1 =
            Value::Range(Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), false));
        let range2 =
            Value::Range(Range::new(Some(Value::Int(7)), false, Some(Value::Int(10)), false));
        let result =
            eval_binary_op(&range1, &BinaryOperator::RangeAdjacent, &range2, &empty_enum_types())
                .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_array_overlaps_true() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Array(vec![Value::Int(2), Value::Int(3)]);
        let result =
            eval_binary_op(&left, &BinaryOperator::ArrayOverlaps, &right, &empty_enum_types())
                .unwrap();
        assert_eq!(result, Value::Bool(true));
    }
}
