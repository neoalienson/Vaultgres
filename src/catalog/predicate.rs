use super::{EnumTypeDef, TableSchema, Value};
use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub struct PredicateEvaluator;

impl PredicateEvaluator {
    pub fn evaluate(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    ) -> Result<bool, String> {
        Self::evaluate_with_subquery(
            expr,
            tuple,
            schema,
            &|_| Err("Subquery evaluation not supported in this context".to_string()),
            enum_types,
        )
    }

    pub fn evaluate_with_subquery<F>(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    ) -> Result<bool, String>
    where
        F: Fn(&crate::parser::ast::SelectStmt) -> Result<Value, String>,
    {
        Self::evaluate_with_in_subquery(
            expr,
            tuple,
            schema,
            subquery_eval,
            &|_, _| Err("IN subquery not supported in this context".to_string()),
            enum_types,
        )
    }

    pub fn evaluate_with_in_subquery<F, G>(
        expr: &Expr,
        tuple: &[Value],
        schema: &TableSchema,
        subquery_eval: &F,
        in_subquery_eval: &G,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
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
                enum_types,
            ),
            Expr::UnaryOp { op, expr } => Self::evaluate_unary_op_with_in_subquery(
                op,
                expr,
                tuple,
                schema,
                subquery_eval,
                in_subquery_eval,
                enum_types,
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
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
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
                            if Self::values_equal_with_enum_support(&left_val, &val, enum_types)? {
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
                    enum_types,
                )?;
                let right_result = Self::evaluate_with_in_subquery(
                    right,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                    enum_types,
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
                    enum_types,
                )?;
                let right_result = Self::evaluate_with_in_subquery(
                    right,
                    tuple,
                    schema,
                    subquery_eval,
                    in_subquery_eval,
                    enum_types,
                )?;
                Ok(left_result || right_result)
            }
            BinaryOperator::ArrayContains
            | BinaryOperator::ArrayContainedBy
            | BinaryOperator::ArrayOverlaps
            | BinaryOperator::ArrayConcat => {
                let left_val =
                    Self::evaluate_expr_with_subquery(left, tuple, schema, subquery_eval)?;
                let right_val =
                    Self::evaluate_expr_with_subquery(right, tuple, schema, subquery_eval)?;
                Self::eval_array_op(&left_val, op, &right_val)
            }
            _ => {
                let left_val =
                    Self::evaluate_expr_with_subquery(left, tuple, schema, subquery_eval)?;
                let right_val =
                    Self::evaluate_expr_with_subquery(right, tuple, schema, subquery_eval)?;
                Self::compare_values(&left_val, op, &right_val, enum_types)
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
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
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
                    enum_types,
                )?;
                Ok(!result)
            }
            _ => Err("Unsupported unary operator".to_string()),
        }
    }

    fn compare_values(
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    ) -> Result<bool, String> {
        match op {
            BinaryOperator::Equals => {
                if let Some(result) = Self::compare_enum_text(left, right, enum_types)? {
                    return Ok(result);
                }
                if let Some(result) = Self::compare_enum_text(right, left, enum_types)? {
                    return Ok(result);
                }
                Ok(left == right)
            }
            BinaryOperator::NotEquals => {
                log::debug!("compare_values: NotEquals left={}, right={}", left, right);
                if let Some(result) = Self::compare_enum_text(left, right, enum_types)? {
                    log::debug!("compare_values: NotEquals enum compare gave {}", result);
                    return Ok(!result);
                }
                if let Some(result) = Self::compare_enum_text(right, left, enum_types)? {
                    log::debug!("compare_values: NotEquals enum compare (rev) gave {}", result);
                    return Ok(!result);
                }
                log::debug!("compare_values: NotEquals falling back to direct comparison");
                Ok(left != right)
            }
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

    fn compare_enum_text(
        enum_val: &Value,
        text_val: &Value,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    ) -> Result<Option<bool>, String> {
        match (enum_val, text_val) {
            (Value::Enum(e), Value::Text(s)) => {
                let types = enum_types.read().unwrap();
                log::debug!(
                    "compare_enum_text: enum_val={}, text_val={}, type_name={}",
                    enum_val,
                    s,
                    e.type_name
                );
                if let Some(def) = types.get(&e.type_name) {
                    log::debug!("compare_enum_text: found def with {} labels", def.labels.len());
                    if let Some(label) = def.labels.get(e.index as usize) {
                        log::debug!(
                            "compare_enum_text: label at index {} = '{}', comparing to '{}'",
                            e.index,
                            label,
                            s
                        );
                        return Ok(Some(label == s));
                    } else {
                        log::debug!("compare_enum_text: no label at index {}", e.index);
                    }
                } else {
                    log::debug!(
                        "compare_enum_text: type '{}' not found in enum_types",
                        e.type_name
                    );
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn values_equal_with_enum_support(
        left: &Value,
        right: &Value,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    ) -> Result<bool, String> {
        if left == right {
            return Ok(true);
        }
        if let Some(result) = Self::compare_enum_text(left, right, enum_types)? {
            return Ok(result);
        }
        if let Some(result) = Self::compare_enum_text(right, left, enum_types)? {
            return Ok(result);
        }
        Ok(false)
    }

    fn eval_array_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<bool, String> {
        match op {
            BinaryOperator::ArrayContains => match (left, right) {
                (Value::Array(arr), elem) => {
                    for item in arr {
                        if item == elem {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                _ => Err("Array contains (@>) requires array on left side".to_string()),
            },
            BinaryOperator::ArrayContainedBy => match (left, right) {
                (Value::Array(left_arr), Value::Array(right_arr)) => {
                    for elem in left_arr {
                        let mut found = false;
                        for item in right_arr {
                            if item == elem {
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                _ => Err("Array contained by (<@) requires arrays on both sides".to_string()),
            },
            BinaryOperator::ArrayOverlaps => match (left, right) {
                (Value::Array(l), Value::Array(r)) => {
                    for left_item in l {
                        for right_item in r {
                            if left_item == right_item {
                                return Ok(true);
                            }
                        }
                    }
                    Ok(false)
                }
                _ => Err("Array overlaps (&&) requires arrays on both sides".to_string()),
            },
            BinaryOperator::ArrayConcat => {
                Err("Array concat (||) not supported in WHERE clause".to_string())
            }
            _ => Err("Not an array operator".to_string()),
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
            Expr::Null => Ok(Value::Null),
            Expr::Subquery(select) => {
                log::debug!("Evaluating subquery for table: {}", select.from);
                subquery_eval(select)
            }
            Expr::List(_) => Err("List not evaluable as value".to_string()),
            Expr::Array(arr) => {
                let mut values = Vec::new();
                for elem in arr {
                    values.push(Self::evaluate_expr_with_subquery(
                        elem,
                        tuple,
                        schema,
                        subquery_eval,
                    )?);
                }
                Ok(Value::Array(values))
            }
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
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    ) -> Result<bool, String> {
        // This is a simplified implementation.
        // It's similar to evaluate_with_subquery but uses a HashMap for tuple access.
        // For now, subquery_eval and in_subquery_eval are not supported in this context.
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expr_tuple_map(left, tuple_map, schema)?;
                let right_val = Self::evaluate_expr_tuple_map(right, tuple_map, schema)?;
                Self::compare_values(&left_val, op, &right_val, enum_types)
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let result = Self::evaluate_tuple_map(expr, tuple_map, schema, enum_types)?;
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
    use crate::catalog::EnumValue;
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

    fn empty_enum_types() -> Arc<RwLock<HashMap<String, EnumTypeDef>>> {
        Arc::new(RwLock::new(HashMap::new()))
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
    }

    #[test]
    fn test_evaluate_is_null() {
        let schema = create_test_schema();
        let tuple = vec![Value::Null, Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::IsNull(Box::new(Expr::Column("id".to_string())));
        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
    }

    #[test]
    fn test_evaluate_is_not_null() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(25)];

        let expr = Expr::IsNotNull(Box::new(Expr::Column("id".to_string())));
        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap());
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap_err(),
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap_err(),
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap_err(),
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap_err(),
            "LIKE requires text values"
        );
    }

    #[test]
    fn test_evaluate_is_null_false() {
        let schema = create_test_schema();
        let tuple = vec![Value::Int(1)];
        let expr = Expr::IsNull(Box::new(Expr::Column("id".to_string())));
        assert!(
            !PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap()
        );
    }

    #[test]
    fn test_evaluate_is_not_null_false() {
        let schema = create_test_schema();
        let tuple = vec![Value::Null];
        let expr = Expr::IsNotNull(Box::new(Expr::Column("id".to_string())));
        assert!(
            !PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap()
        );
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

        assert!(PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).is_err());
        assert_eq!(
            PredicateEvaluator::evaluate(&expr, &tuple, &schema, &empty_enum_types()).unwrap_err(),
            "IN subquery not supported in this context"
        );
    }

    #[test]
    fn test_evaluate_enum_not_equals() {
        let enum_types = Arc::new(RwLock::new({
            let mut m = HashMap::new();
            m.insert(
                "order_status".to_string(),
                EnumTypeDef {
                    type_name: "order_status".to_string(),
                    labels: vec![
                        "pending".to_string(),
                        "processing".to_string(),
                        "shipped".to_string(),
                        "delivered".to_string(),
                        "cancelled".to_string(),
                    ],
                },
            );
            m
        }));

        let schema = TableSchema::new(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("status".to_string(), DataType::Enum("order_status".to_string())),
            ],
        );

        // Test with 'shipped' (index 2), comparing to 'cancelled' (index 4)
        let tuple = vec![
            Value::Int(1),
            Value::Enum(EnumValue { type_name: "order_status".to_string(), index: 2 }),
        ];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::NotEquals,
            right: Box::new(Expr::String("cancelled".to_string())),
        };

        let result = PredicateEvaluator::evaluate(&expr, &tuple, &schema, &enum_types);
        println!("Enum NotEquals result: {:?}", result);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expected status != 'cancelled' to be true for 'shipped'");
    }

    #[test]
    fn test_evaluate_enum_equals() {
        let enum_types = Arc::new(RwLock::new({
            let mut m = HashMap::new();
            m.insert(
                "order_status".to_string(),
                EnumTypeDef {
                    type_name: "order_status".to_string(),
                    labels: vec![
                        "pending".to_string(),
                        "processing".to_string(),
                        "shipped".to_string(),
                        "delivered".to_string(),
                        "cancelled".to_string(),
                    ],
                },
            );
            m
        }));

        let schema = TableSchema::new(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("status".to_string(), DataType::Enum("order_status".to_string())),
            ],
        );

        // Test with 'shipped' (index 2), comparing to 'shipped' (index 2)
        let tuple = vec![
            Value::Int(1),
            Value::Enum(EnumValue { type_name: "order_status".to_string(), index: 2 }),
        ];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("shipped".to_string())),
        };

        let result = PredicateEvaluator::evaluate(&expr, &tuple, &schema, &enum_types);
        println!("Enum Equals result: {:?}", result);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expected status = 'shipped' to be true for 'shipped'");
    }
}
