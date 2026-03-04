use super::operators::executor::{ExecutorError, Tuple};
use crate::catalog::{string_functions, Catalog, Value};
use crate::parser::ast::{BinaryOperator, Expr, SelectStmt, UnaryOperator};
use std::sync::Arc;

pub struct Eval;

impl Eval {
    /// Evaluate an expression given a tuple (HashMap of column values)
    pub fn eval_expr(expr: &Expr, tuple: &Tuple) -> Result<Value, ExecutorError> {
        Self::eval_expr_with_catalog(expr, tuple, None)
    }

    /// Evaluate an expression with optional catalog for subqueries
    pub fn eval_expr_with_catalog(
        expr: &Expr,
        tuple: &Tuple,
        catalog: Option<&Catalog>,
    ) -> Result<Value, ExecutorError> {
        match expr {
            Expr::Column(name) => {
                tuple.get(name).cloned().ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Star => Err(ExecutorError::UnsupportedExpression(
                "* not allowed in this context".to_string(),
            )),

            // Binary operations
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::eval_expr(left, tuple)?;

                // Special handling for IN with List
                if *op == BinaryOperator::In {
                    if let Expr::List(values) = right.as_ref() {
                        let mut found = false;
                        for val_expr in values {
                            if let Ok(val) = Self::eval_expr(val_expr, tuple) {
                                if val == left_val {
                                    found = true;
                                    break;
                                }
                            }
                        }
                        return Ok(Value::Bool(found));
                    }
                }

                let right_val = Self::eval_expr(right, tuple)?;
                Self::eval_binary_op(&left_val, op, &right_val)
            }

            // Unary operations
            Expr::UnaryOp { op, expr } => {
                let val = Self::eval_expr(expr, tuple)?;
                Self::eval_unary_op(op, &val)
            }

            // NULL checks
            Expr::IsNull(inner) => {
                let val = Self::eval_expr(inner, tuple)?;
                Ok(Value::Bool(matches!(val, Value::Null)))
            }
            Expr::IsNotNull(inner) => {
                let val = Self::eval_expr(inner, tuple)?;
                Ok(Value::Bool(!matches!(val, Value::Null)))
            }

            // Function calls
            Expr::FunctionCall { name, args } => {
                let mut evaluated_args = Vec::new();
                for arg in args {
                    evaluated_args.push(Self::eval_expr(arg, tuple)?);
                }
                Self::eval_function(name, evaluated_args)
            }

            // Aggregates - these should be handled by HashAggExecutor, not here
            Expr::Aggregate { func: _, arg } => {
                // In a proper execution model, aggregates are handled by a separate aggregator
                // For now, we evaluate the argument
                // Special handling for COUNT(*) - just return 1 to count the row
                if matches!(arg.as_ref(), Expr::Star) {
                    Ok(Value::Int(1))
                } else {
                    Self::eval_expr(arg, tuple)
                }
            }

            // CASE expression
            Expr::Case { conditions, else_expr } => {
                for (condition, result) in conditions {
                    let cond_val = Self::eval_expr(condition, tuple)?;
                    if let Value::Bool(true) = cond_val {
                        return Self::eval_expr(result, tuple);
                    }
                }
                if let Some(else_expr) = else_expr {
                    Self::eval_expr(else_expr, tuple)
                } else {
                    Ok(Value::Null)
                }
            }

            // Aliased expressions
            Expr::Alias { expr, alias: _ } => Self::eval_expr(expr, tuple),

            // Unsupported in this context
            Expr::QualifiedColumn { table, column } => {
                // Try to find by column name only (ignore table qualifier for now)
                tuple
                    .get(column)
                    .cloned()
                    .or_else(|| tuple.get(&format!("{}.{}", table, column)).cloned())
                    .ok_or_else(|| ExecutorError::ColumnNotFound(format!("{}.{}", table, column)))
            }
            Expr::Parameter(_) => Err(ExecutorError::UnsupportedExpression(
                "Parameters not supported in this context".to_string(),
            )),
            Expr::List(_) => Err(ExecutorError::UnsupportedExpression(
                "List not supported in this context".to_string(),
            )),
            Expr::Subquery(stmt) => {
                // Execute scalar subquery
                if let Some(catalog) = catalog {
                    Self::eval_scalar_subquery(catalog, stmt)
                } else {
                    Err(ExecutorError::UnsupportedExpression(
                        "Subqueries require catalog".to_string(),
                    ))
                }
            }
            Expr::Window { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions not supported in this context".to_string(),
            )),
        }
    }

    /// Evaluate a scalar subquery (returns single value)
    fn eval_scalar_subquery(catalog: &Catalog, stmt: &SelectStmt) -> Result<Value, ExecutorError> {
        // Use select_with_catalog for proper subquery execution
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
                    // Single row, single column - scalar result
                    Ok(rows[0][0].clone())
                } else if rows.len() == 1 {
                    // Single row, multiple columns - return first column
                    Ok(rows[0][0].clone())
                } else {
                    // Multiple rows - return first value of first row (typical scalar subquery behavior)
                    Ok(rows[0][0].clone())
                }
            }
            Err(e) => {
                Err(ExecutorError::InternalError(format!("Subquery execution failed: {}", e)))
            }
        }
    }

    /// Evaluate a binary operation
    fn eval_binary_op(
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value, ExecutorError> {
        // Handle NULL propagation
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            // AND and OR have special NULL handling
            match op {
                BinaryOperator::And => {
                    // NULL AND false = false, NULL AND true = NULL
                    if let Value::Bool(false) = left {
                        return Ok(Value::Bool(false));
                    }
                    if let Value::Bool(false) = right {
                        return Ok(Value::Bool(false));
                    }
                    return Ok(Value::Null);
                }
                BinaryOperator::Or => {
                    // NULL OR true = true, NULL OR false = NULL
                    if let Value::Bool(true) = left {
                        return Ok(Value::Bool(true));
                    }
                    if let Value::Bool(true) = right {
                        return Ok(Value::Bool(true));
                    }
                    return Ok(Value::Null);
                }
                _ => return Ok(Value::Null),
            }
        }

        match op {
            BinaryOperator::Equals => Ok(Value::Bool(left == right)),
            BinaryOperator::NotEquals => Ok(Value::Bool(left != right)),

            // Comparison operators
            BinaryOperator::LessThan => {
                Self::compare_values(left, right, |cmp| cmp == std::cmp::Ordering::Less)
            }
            BinaryOperator::LessThanOrEqual => {
                Self::compare_values(left, right, |cmp| cmp != std::cmp::Ordering::Greater)
            }
            BinaryOperator::GreaterThan => {
                Self::compare_values(left, right, |cmp| cmp == std::cmp::Ordering::Greater)
            }
            BinaryOperator::GreaterThanOrEqual => {
                Self::compare_values(left, right, |cmp| cmp != std::cmp::Ordering::Less)
            }

            // Logical operators
            BinaryOperator::And => match (left, right) {
                (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l && *r)),
                _ => Err(ExecutorError::TypeMismatch("AND requires boolean operands".to_string())),
            },
            BinaryOperator::Or => match (left, right) {
                (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l || *r)),
                _ => Err(ExecutorError::TypeMismatch("OR requires boolean operands".to_string())),
            },

            // Arithmetic operators
            BinaryOperator::Add => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Value::Int(*l + *r)),
                (Value::Text(l), Value::Text(r)) => Ok(Value::Text(format!("{}{}", l, r))),
                _ => Err(ExecutorError::TypeMismatch(
                    "ADD requires numeric or text operands".to_string(),
                )),
            },
            BinaryOperator::StringConcat => {
                let l_str = Self::value_to_string(left);
                let r_str = Self::value_to_string(right);
                Ok(Value::Text(format!("{}{}", l_str, r_str)))
            }

            // Other operators
            BinaryOperator::Like => Self::eval_like(left, right, false),
            BinaryOperator::ILike => Self::eval_like(left, right, true),
            BinaryOperator::In => {
                // IN operator: left IN (value1, value2, ...) or left IN (subquery)
                // right should be a List or Subquery
                match right {
                    Value::Text(list_str) => {
                        // Parse comma-separated list from string (legacy format)
                        let items: Vec<&str> = list_str.split(',').map(|s| s.trim()).collect();
                        let left_str = Self::value_to_string(left);
                        Ok(Value::Bool(items.contains(&left_str.as_str())))
                    }
                    _ => Err(ExecutorError::UnsupportedExpression(
                        "IN operator requires a list".to_string(),
                    )),
                }
            }
            BinaryOperator::Between => {
                // BETWEEN should have been converted to AND of comparisons by the parser
                // If it reaches here, it's an error
                Err(ExecutorError::InternalError(
                    "BETWEEN should be converted by parser".to_string(),
                ))
            }
            BinaryOperator::Any | BinaryOperator::All | BinaryOperator::Some => {
                Err(ExecutorError::UnsupportedExpression(
                    "ANY/ALL/SOME operators require subquery".to_string(),
                ))
            }
        }
    }

    /// Evaluate a unary operation
    fn eval_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value, ExecutorError> {
        match op {
            UnaryOperator::Not => match val {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err(ExecutorError::TypeMismatch("NOT requires boolean operand".to_string())),
            },
            UnaryOperator::Minus => match val {
                Value::Int(n) => Ok(Value::Int(-n)),
                _ => Err(ExecutorError::TypeMismatch(
                    "Unary minus requires numeric operand".to_string(),
                )),
            },
        }
    }

    /// Helper for comparison operations
    fn compare_values<F>(left: &Value, right: &Value, cmp_fn: F) -> Result<Value, ExecutorError>
    where
        F: FnOnce(std::cmp::Ordering) -> bool,
    {
        match (left, right) {
            (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(cmp_fn(l.cmp(r)))),
            (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(cmp_fn(l.cmp(r)))),
            _ => {
                Err(ExecutorError::TypeMismatch("Comparison requires compatible types".to_string()))
            }
        }
    }

    /// Evaluate LIKE pattern matching
    fn eval_like(
        left: &Value,
        right: &Value,
        case_insensitive: bool,
    ) -> Result<Value, ExecutorError> {
        let text = match left {
            Value::Text(s) => s,
            _ => return Err(ExecutorError::TypeMismatch("LIKE requires text operand".to_string())),
        };

        let pattern = match right {
            Value::Text(s) => s,
            _ => return Err(ExecutorError::TypeMismatch("LIKE requires text pattern".to_string())),
        };

        // Convert SQL LIKE pattern to regex
        let regex_pattern = regex::escape(pattern).replace('%', ".*").replace('_', ".");

        let regex = if case_insensitive {
            regex::Regex::new(&format!("(?i)^{}$", regex_pattern))
        } else {
            regex::Regex::new(&format!("^{}$", regex_pattern))
        }
        .map_err(|e| ExecutorError::InternalError(format!("Invalid LIKE pattern: {}", e)))?;

        Ok(Value::Bool(regex.is_match(text)))
    }

    /// Convert a Value to string for concatenation
    fn value_to_string(val: &Value) -> String {
        match val {
            Value::Text(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
            _ => format!("{:?}", val),
        }
    }

    /// Evaluate a function call
    fn eval_function(name: &str, args: Vec<Value>) -> Result<Value, ExecutorError> {
        match name.to_uppercase().as_str() {
            "UPPER" => {
                if args.len() != 1 {
                    return Err(ExecutorError::TypeMismatch(
                        "UPPER takes one argument".to_string(),
                    ));
                }
                string_functions::StringFunctions::upper(args[0].clone())
                    .map_err(ExecutorError::TypeMismatch)
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(ExecutorError::TypeMismatch(
                        "LOWER takes one argument".to_string(),
                    ));
                }
                string_functions::StringFunctions::lower(args[0].clone())
                    .map_err(ExecutorError::TypeMismatch)
            }
            "LENGTH" => {
                if args.len() != 1 {
                    return Err(ExecutorError::TypeMismatch(
                        "LENGTH takes one argument".to_string(),
                    ));
                }
                string_functions::StringFunctions::length(args[0].clone())
                    .map_err(ExecutorError::TypeMismatch)
            }
            "COALESCE" => {
                // Return first non-null value
                for arg in args {
                    if !matches!(arg, Value::Null) {
                        return Ok(arg);
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                // Return NULL if args are equal, otherwise return first arg
                if args.len() != 2 {
                    return Err(ExecutorError::TypeMismatch(
                        "NULLIF takes two arguments".to_string(),
                    ));
                }
                if args[0] == args[1] {
                    Ok(Value::Null)
                } else {
                    Ok(args[0].clone())
                }
            }
            _ => Err(ExecutorError::FunctionNotFound(format!("Function '{}' not found", name))),
        }
    }
}
