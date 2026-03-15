use super::{TableSchema, Value};
use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
use std::collections::HashMap;

pub struct PredicateEvaluator;

impl PredicateEvaluator {
    pub fn evaluate(expr: &Expr, tuple: &[Value], schema: &TableSchema) -> Result<bool, String> {
        Self::evaluate_with_subquery(expr, tuple, schema, &|_| {
            Err("Subquery evaluation not supported in this context".to_string())
        })
    }

    pub fn evaluate_with_subquery<F>(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
    ) -> Result<bool, String>
    where
        F: Fn(&crate::parser::ast::SelectStmt) -> Result<Value, String>,
    {
        Self::evaluate_with_in_subquery(expr, tuple, schema, subquery_eval, &|_, _| {
            Err("IN subquery not supported in this context".to_string())
        })
    }

    pub fn evaluate_with_in_subquery<F, G>(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
        in_subquery_eval: &G,
    ) -> Result<bool, String>
    where
        F: Fn(&crate::parser::ast::SelectStmt) -> Result<Value, String>,
        G: Fn(&crate::parser::ast::SelectStmt, &Value) -> Result<bool, String>,
    {
        match expr {
            Expr::BinaryOp { left, op, right } => Self::evaluate_binary_op_with_in_subquery(
                left,
                op,
                right,
                tuple,
                schema,
                subquery_eval,
                in_subquery_eval,
            ),
            Expr::UnaryOp { op, expr } => Self::evaluate_unary_op_with_in_subquery(
                op,
                expr,
                tuple,
                schema,
                subquery_eval,
                in_subquery_eval,
            ),
            Expr::IsNull(expr) => {
                let val = Self::evaluate_expr_with_subquery(expr, tuple, schema, subquery_eval)?;
                Ok(matches!(val, Value::Null))
            }
            Expr::IsNotNull(expr) => {
                let val = Self::evaluate_expr_with_subquery(expr, tuple, schema, subquery_eval)?;
                Ok(!matches!(val, Value::Null))
            }
            _ => Err("Unsupported predicate expression".to_string()),
        }
    }

    fn evaluate_binary_op_with_in_subquery<F, G>(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
        in_subquery_eval: &G,
    ) -> Result<bool, String>
    where
        F: Fn(&crate::parser::ast::SelectStmt) -> Result<Value, String>,
        G: Fn(&crate::parser::ast::SelectStmt, &Value) -> Result<bool, String>,
    {
        match op {
            BinaryOperator::In => {
                let left_val =
                    Self::evaluate_expr_with_subquery(left, tuple, schema, subquery_eval)?;

                match right {
                    Expr::List(values) => {
                        for val_expr in values {
                            let val = Self::evaluate_expr_with_subquery(
                                val_expr,
                                tuple,
                                schema,
                                subquery_eval,
                            )?;
                            if left_val == val {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    }
                    Expr::Subquery(select) => {
                        log::debug!("Evaluating IN subquery");
                        in_subquery_eval(select, &left_val)
                    }
                    _ => Err("IN requires list or subquery".to_string()),
                }
            }
            BinaryOperator::Between => {
                let left_val =
                    Self::evaluate_expr_with_subquery(left, tuple, schema, subquery_eval)?;
                if let Expr::List(values) = right {
                    if values.len() == 2 {
                        let lower = Self::evaluate_expr_with_subquery(
                            &values[0],
                            tuple,
                            schema,
                            subquery_eval,
                        )?;
                        let upper = Self::evaluate_expr_with_subquery(
                            &values[1],
                            tuple,
                            schema,
                            subquery_eval,
                        )?;
                        return Ok(left_val >= lower && left_val <= upper);
                    }
                }
                Err("BETWEEN requires two values".to_string())
            }
            BinaryOperator::And => {
                let left_result = Self::evaluate_with_in_subquery(
                    left,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                )?;
                let right_result = Self::evaluate_with_in_subquery(
                    right,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                )?;
                Ok(left_result && right_result)
            }
            BinaryOperator::Or => {
                let left_result = Self::evaluate_with_in_subquery(
                    left,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                )?;
                let right_result = Self::evaluate_with_in_subquery(
                    right,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                )?;
                Ok(left_result || right_result)
            }
            _ => {
                let left_val =
                    Self::evaluate_expr_with_subquery(left, tuple, schema, subquery_eval)?;
                let right_val =
                    Self::evaluate_expr_with_subquery(right, tuple, schema, subquery_eval)?;
                Self::compare_values(&left_val, op, &right_val)
            }
        }
    }

    fn evaluate_unary_op_with_in_subquery<F, G>(
        op: &UnaryOperator,
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
        in_subquery_eval: &G,
    ) -> Result<bool, String>
    where
        F: Fn(&crate::parser::ast::SelectStmt) -> Result<Value, String>,
        G: Fn(&crate::parser::ast::SelectStmt, &Value) -> Result<bool, String>,
    {
        match op {
            UnaryOperator::Not => {
                let result = Self::evaluate_with_in_subquery(
                    expr,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                )?;
                Ok(!result)
            }
            _ => Err("Unsupported unary operator".to_string()),
        }
    }

    fn compare_values(left: &Value, op: &BinaryOperator, right: &Value) -> Result<bool, String> {
        match op {
            BinaryOperator::Equals => Ok(left == right),
            BinaryOperator::NotEquals => Ok(left != right),
            BinaryOperator::LessThan => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(l < r),
                (Value::Text(l), Value::Text(r)) => Ok(l < r),
                _ => Err("Type mismatch in comparison".to_string()),
            },
            BinaryOperator::LessThanOrEqual => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(l <= r),
                (Value::Text(l), Value::Text(r)) => Ok(l <= r),
                _ => Err("Type mismatch in comparison".to_string()),
            },
            BinaryOperator::GreaterThan => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(l > r),
                (Value::Text(l), Value::Text(r)) => Ok(l > r),
                _ => Err("Type mismatch in comparison".to_string()),
            },
            BinaryOperator::GreaterThanOrEqual => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(l >= r),
                (Value::Text(l), Value::Text(r)) => Ok(l >= r),
                _ => Err("Type mismatch in comparison".to_string()),
            },
            BinaryOperator::Like => match (left, right) {
                (Value::Text(s), Value::Text(pattern)) => Ok(s.contains(&pattern.replace('%', ""))),
                _ => Err("LIKE requires text values".to_string()),
            },
            _ => Err("Unsupported comparison operator".to_string()),
        }
    }

    pub fn evaluate_expr_with_subquery<F>(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
    ) -> Result<Value, String>
    where
        F: Fn(&crate::parser::ast::SelectStmt) -> Result<Value, String>,
    {
        log::trace!("evaluate_expr_with_subquery: expr variant={:?}", std::mem::discriminant(expr));
        match expr {
            Expr::Column(name) => {
                // Handle table-prefixed column names (e.g., "o.total" -> "total")
                let lookup_name = if let Some(dot_pos) = name.find('.') {
                    &name[dot_pos + 1..]
                } else {
                    name.as_str()
                };
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == lookup_name)
                    .ok_or_else(|| format!("Column '{}' not found", name))?;
                Ok(tuple[idx].clone())
            }
            Expr::QualifiedColumn { table: _, column } => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| format!("Column '{}' not found", column))?;
                Ok(tuple[idx].clone())
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Subquery(select) => {
                log::debug!("Evaluating subquery for table: {}", select.from);
                subquery_eval(select)
            }
            Expr::List(_) => Err("List not evaluable as value".to_string()),
            _ => {
                log::warn!("Unsupported expression type in predicate evaluation");
                Err("Unsupported expression".to_string())
            }
        }
    }

    pub fn evaluate_expr(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
    ) -> Result<Value, String> {
        Self::evaluate_expr_with_subquery(expr, tuple, schema, &|_| {
            Err("Subquery evaluation not supported in this context".to_string())
        })
    }

    pub fn evaluate_tuple_map(
        expr: &Expr,
        tuple_map: &HashMap<String, Value>,
        schema: &TableSchema,
    ) -> Result<bool, String> {
        // This is a simplified implementation.
        // It's similar to evaluate_with_subquery but uses a HashMap for tuple access.
        // For now, subquery_eval and in_subquery_eval are not supported in this context.
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expr_tuple_map(left, tuple_map, schema)?;
                let right_val = Self::evaluate_expr_tuple_map(right, tuple_map, schema)?;
                Self::compare_values(&left_val, op, &right_val)
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let result = Self::evaluate_tuple_map(expr, tuple_map, schema)?;
                    Ok(!result)
                }
                _ => Err("Unsupported unary operator".to_string()),
            },
            Expr::IsNull(expr) => {
                let val = Self::evaluate_expr_tuple_map(expr, tuple_map, schema)?;
                Ok(matches!(val, Value::Null))
            }
            Expr::IsNotNull(expr) => {
                let val = Self::evaluate_expr_tuple_map(expr, tuple_map, schema)?;
                Ok(!matches!(val, Value::Null))
            }
            _ => Err("Unsupported predicate expression for tuple_map".to_string()),
        }
    }

    fn evaluate_expr_tuple_map(
        expr: &Expr,
        tuple_map: &HashMap<String, Value>,
        _schema: &TableSchema,
    ) -> Result<Value, String> {
        match expr {
            Expr::Column(name) => tuple_map
                .get(name)
                .cloned()
                .ok_or_else(|| format!("Column '{}' not found in tuple map", name)),
            Expr::QualifiedColumn { table: _, column } => tuple_map
                .get(column)
                .cloned()
                .ok_or_else(|| format!("Column '{}' not found in tuple map", column)),
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            _ => {
                Err("Unsupported expression type in predicate evaluation for tuple_map".to_string())
            }
        }
    }

    pub fn evaluate_having(expr: &Expr, row: &[Value]) -> Result<bool, String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = match **left {
                    Expr::Number(n) => Value::Int(n),
                    _ => row.first().cloned().unwrap_or(Value::Int(0)),
                };
                let right_val = match **right {
                    Expr::Number(n) => Value::Int(n),
                    _ => Value::Int(0),
                };

                match op {
                    BinaryOperator::GreaterThan => Ok(left_val > right_val),
                    BinaryOperator::GreaterThanOrEqual => Ok(left_val >= right_val),
                    BinaryOperator::LessThan => Ok(left_val < right_val),
                    BinaryOperator::LessThanOrEqual => Ok(left_val <= right_val),
                    BinaryOperator::Equals => Ok(left_val == right_val),
                    BinaryOperator::NotEquals => Ok(left_val != right_val),
                    _ => Ok(false),
                }
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::ColumnDef;
    use crate::parser::ast::DataType;

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("age".to_string(), DataType::Int),
            ],
        )
    }

    #[test]
    fn test_evaluate_equals() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_not_operator() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(2)),
            }),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_is_null() {
        let schema = create_test_schema();
        let tuple = vec![Value::Null, Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::IsNull(Box::new(Expr::Column("id".to_string())));
        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_is_not_null() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::IsNotNull(Box::new(Expr::Column("id".to_string())));
        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_in_operator() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(2), Value::Text("Bob".to_string()), Value::Int(30)];

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![Expr::Number(1), Expr::Number(2), Expr::Number(3)])),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_between() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: BinaryOperator::Between,
            right: Box::new(Expr::List(vec![Expr::Number(20), Expr::Number(30)])),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_like() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("%lic%".to_string())),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_and_or() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(1)),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(20)),
            }),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_in_operator_not_list() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(2)];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::Number(1)), // Not a list
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap_err(),
            "IN requires list or subquery"
        );
    }

    #[test]
    fn test_evaluate_between_invalid_values() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: BinaryOperator::Between,
            right: Box::new(Expr::List(vec![Expr::Number(20)])), // Only one value
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap_err(),
            "BETWEEN requires two values"
        );
    }

    #[test]
    fn test_evaluate_unsupported_unary_operator() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1)];
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Column("id".to_string())),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap_err(),
            "Unsupported unary operator"
        );
    }

    #[test]
    fn test_evaluate_like_non_text() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("1".to_string())),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap_err(),
            "LIKE requires text values"
        );
    }

    #[test]
    fn test_evaluate_is_null_false() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1)];
        let expr = Expr::IsNull(Box::new(Expr::Column("id".to_string())));
        assert!(!PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_is_not_null_false() {
        let schema = create_test_schema();
        let tuple = vec![Value::Null];
        let expr = Expr::IsNotNull(Box::new(Expr::Column("id".to_string())));
        assert!(!PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap());
    }

    #[test]
    fn test_evaluate_subquery_default_error() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1)];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::Subquery(Box::new(crate::parser::ast::SelectStmt {
                columns: vec![],
                from: "other".to_string(),
                joins: vec![],
                table_alias: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: false,
            }))),
        };

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema).unwrap_err(),
            "IN subquery not supported in this context"
        );
    }
}
