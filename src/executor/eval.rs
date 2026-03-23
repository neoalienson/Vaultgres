use super::operators::executor::{ExecutorError, Tuple};
use crate::catalog::{Catalog, Value, string_functions};
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
                // Handle table-prefixed column names (e.g., "c.id" -> look up "c.id" directly)
                // For unprefixed names, try direct lookup first, then search for unique match
                if name.contains('.') {
                    // Prefixed column - look up directly
                    tuple
                        .get(name.as_str())
                        .cloned()
                        .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
                } else {
                    // Unprefixed column - try direct lookup first
                    if let Some(value) = tuple.get(name.as_str()) {
                        return Ok(value.clone());
                    }
                    // Search for unique match with any prefix
                    let matches: Vec<_> = tuple
                        .iter()
                        .filter(|(k, _)| k.ends_with(&format!(".{}", name)) || *k == name)
                        .collect();

                    if matches.is_empty() {
                        Err(ExecutorError::ColumnNotFound(name.clone()))
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
            Expr::QualifiedColumn { table, column } => {
                // For qualified columns, look up "table.column" directly
                let qualified_name = format!("{}.{}", table, column);
                tuple
                    .get(&qualified_name)
                    .cloned()
                    .or_else(|| tuple.get(column).cloned()) // Fallback to unqualified
                    .ok_or(ExecutorError::ColumnNotFound(qualified_name))
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Null => Ok(Value::Null),
            Expr::Star => Err(ExecutorError::UnsupportedExpression(
                "* not allowed in this context".to_string(),
            )),

            // Binary operations
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::eval_expr_with_catalog(left, tuple, catalog)?;

                // Special handling for IN with List or Subquery
                if *op == BinaryOperator::In {
                    if let Expr::List(values) = right.as_ref() {
                        let mut found = false;
                        for val_expr in values {
                            if let Ok(val) = Self::eval_expr_with_catalog(val_expr, tuple, catalog)
                            {
                                if val == left_val {
                                    found = true;
                                    break;
                                }
                            }
                        }
                        return Ok(Value::Bool(found));
                    }
                    // Handle IN with subquery
                    if let Expr::Subquery(stmt) = right.as_ref() {
                        if let Some(catalog) = catalog {
                            let _subquery_result = Self::eval_scalar_subquery(catalog, stmt);
                            // For IN subquery, we need to check if left_val is in the result set
                            // Execute the subquery and get all results
                            let catalog_arc = Arc::new(catalog.clone());
                            let result = crate::catalog::Catalog::select_with_catalog(
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
                                    let found =
                                        rows.iter().any(|row| row.len() == 1 && row[0] == left_val);
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

                let right_val = Self::eval_expr_with_catalog(right, tuple, catalog)?;
                Self::eval_binary_op(&left_val, op, &right_val)
            }

            // Unary operations
            Expr::UnaryOp { op, expr } => {
                let val = Self::eval_expr_with_catalog(expr, tuple, catalog)?;
                Self::eval_unary_op(op, &val)
            }

            // NULL checks
            Expr::IsNull(inner) => {
                let val = Self::eval_expr_with_catalog(inner, tuple, catalog)?;
                Ok(Value::Bool(matches!(val, Value::Null)))
            }
            Expr::IsNotNull(inner) => {
                let val = Self::eval_expr_with_catalog(inner, tuple, catalog)?;
                Ok(Value::Bool(!matches!(val, Value::Null)))
            }

            // Function calls
            Expr::FunctionCall { name, args } => {
                let mut evaluated_args = Vec::new();
                for arg in args {
                    evaluated_args.push(Self::eval_expr_with_catalog(arg, tuple, catalog)?);
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
                    Self::eval_expr_with_catalog(arg, tuple, catalog)
                }
            }

            // CASE expression
            Expr::Case { conditions, else_expr } => {
                for (condition, result) in conditions {
                    let cond_val = Self::eval_expr_with_catalog(condition, tuple, catalog)?;
                    if let Value::Bool(true) = cond_val {
                        return Self::eval_expr_with_catalog(result, tuple, catalog);
                    }
                }
                if let Some(else_expr) = else_expr {
                    Self::eval_expr_with_catalog(else_expr, tuple, catalog)
                } else {
                    Ok(Value::Null)
                }
            }

            // Aliased expressions
            Expr::Alias { expr, alias: _ } => Self::eval_expr_with_catalog(expr, tuple, catalog),

            Expr::Parameter(_) => Err(ExecutorError::UnsupportedExpression(
                "Parameters not supported in this context".to_string(),
            )),
            Expr::List(_) => Err(ExecutorError::UnsupportedExpression(
                "List not supported in this context".to_string(),
            )),
            Expr::Array(_) => Err(ExecutorError::UnsupportedExpression(
                "Array expressions not supported in this context".to_string(),
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
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(*l + *r)),
                (Value::Text(l), Value::Text(r)) => Ok(Value::Text(format!("{}{}", l, r))),
                _ => Err(ExecutorError::TypeMismatch(
                    "ADD requires numeric or text operands".to_string(),
                )),
            },
            BinaryOperator::Subtract => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Value::Int(*l - *r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(*l - *r)),
                _ => Err(ExecutorError::TypeMismatch(
                    "SUBTRACT requires numeric operands".to_string(),
                )),
            },
            BinaryOperator::Multiply => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Value::Int(*l * *r)),
                (Value::Float(l), Value::Float(r)) => Ok(Value::Float(*l * *r)),
                _ => Err(ExecutorError::TypeMismatch(
                    "MULTIPLY requires numeric operands".to_string(),
                )),
            },
            BinaryOperator::Divide => match (left, right) {
                (Value::Int(l), Value::Int(r)) => {
                    if *r == 0 {
                        Err(ExecutorError::DivisionByZero)
                    } else {
                        Ok(Value::Int(*l / *r))
                    }
                }
                (Value::Float(l), Value::Float(r)) => {
                    if *r == 0.0 {
                        Err(ExecutorError::DivisionByZero)
                    } else {
                        Ok(Value::Float(*l / *r))
                    }
                }
                _ => {
                    Err(ExecutorError::TypeMismatch("DIVIDE requires numeric operands".to_string()))
                }
            },
            BinaryOperator::Modulo => match (left, right) {
                (Value::Int(l), Value::Int(r)) => {
                    if *r == 0 {
                        Err(ExecutorError::DivisionByZero)
                    } else {
                        Ok(Value::Int(*l % *r))
                    }
                }
                _ => {
                    Err(ExecutorError::TypeMismatch("MODULO requires integer operands".to_string()))
                }
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

            // JSON operators
            BinaryOperator::JsonExtract => Self::eval_json_extract(left, right, false),
            BinaryOperator::JsonExtractText => Self::eval_json_extract(left, right, true),
            BinaryOperator::JsonPath => Self::eval_json_path(left, right, false),
            BinaryOperator::JsonPathText => Self::eval_json_path(left, right, true),
            BinaryOperator::JsonExists => Self::eval_json_exists(left, right),
            BinaryOperator::JsonExistsAny => Self::eval_json_exists_any(left, right),
            BinaryOperator::JsonExistsAll => Self::eval_json_exists_all(left, right),

            // Array operators
            BinaryOperator::ArrayContains => Self::eval_array_contains(left, right),
            BinaryOperator::ArrayContainedBy => Self::eval_array_contained_by(left, right),
            BinaryOperator::ArrayOverlaps => Self::eval_array_overlaps(left, right),
            BinaryOperator::ArrayConcat => Self::eval_array_concat(left, right),
            BinaryOperator::ArrayAccess => Self::eval_array_access(left, right),
        }
    }

    /// Evaluate JSON extraction operator (-> or ->>)
    fn eval_json_extract(
        left: &Value,
        right: &Value,
        as_text: bool,
    ) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON extract requires JSON or text operand".to_string(),
                ));
            }
        };

        let key = match right {
            Value::Text(k) => k.as_str(),
            Value::Int(i) => &i.to_string(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON extract key must be text or integer".to_string(),
                ));
            }
        };

        match Self::extract_json_field(json_str, key) {
            Some(value) => {
                if as_text {
                    Ok(Value::Text(value.to_string()))
                } else {
                    Ok(Value::Json(value.to_string()))
                }
            }
            None => Ok(Value::Null),
        }
    }

    /// Extract a field from JSON string
    fn extract_json_field(json_str: &str, key: &str) -> Option<String> {
        let json_str = json_str.trim();
        if json_str.starts_with('[') {
            if let Ok(idx) = key.parse::<usize>() {
                return Self::extract_json_array_element(json_str, idx);
            }
            return None;
        }
        if json_str.starts_with('{') {
            return Self::extract_json_object_field(json_str, key);
        }
        None
    }

    /// Extract element from JSON array
    fn extract_json_array_element(json_str: &str, idx: usize) -> Option<String> {
        let json_str = json_str.trim();
        if !json_str.starts_with('[') || !json_str.ends_with(']') {
            return None;
        }
        let content = &json_str[1..json_str.len() - 1];
        if content.trim().is_empty() {
            return None;
        }
        let elements = Self::split_json_array(content);
        if idx >= elements.len() {
            return None;
        }
        Some(elements[idx].trim().to_string())
    }

    /// Split JSON array into elements (simple parser)
    fn split_json_array(content: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;
        let mut in_string = false;
        for (i, c) in content.chars().enumerate() {
            match c {
                '"' => in_string = !in_string,
                '[' | '{' if !in_string => depth += 1,
                ']' | '}' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    result.push(&content[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        result.push(&content[start..]);
        result
    }

    /// Extract field from JSON object
    fn extract_json_object_field(json_str: &str, key: &str) -> Option<String> {
        let json_str = json_str.trim();
        if !json_str.starts_with('{') || !json_str.ends_with('}') {
            return None;
        }
        let content = &json_str[1..json_str.len() - 1];
        if content.trim().is_empty() {
            return None;
        }
        let pairs = Self::split_json_object(content);
        for pair in pairs {
            if let Some((k, v)) = Self::parse_json_pair(pair) {
                if k == key {
                    return Some(v);
                }
            }
        }
        None
    }

    /// Split JSON object into key-value pairs (simple parser)
    fn split_json_object(content: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;
        let mut in_string = false;
        for (i, c) in content.chars().enumerate() {
            match c {
                '"' => in_string = !in_string,
                '{' | '[' if !in_string => depth += 1,
                '}' | ']' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    result.push(&content[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        result.push(&content[start..]);
        result
    }

    /// Parse a JSON key-value pair
    fn parse_json_pair(pair: &str) -> Option<(String, String)> {
        let pair = pair.trim();
        if !pair.starts_with('"') {
            return None;
        }
        let colon_pos = pair.find(':')?;
        let closing_quote_pos = pair[..colon_pos].rfind('"')?;
        let key = pair[1..closing_quote_pos].to_string();
        let raw_value = pair[colon_pos + 1..].trim();
        let value = if raw_value.starts_with('"') {
            let first_quote = raw_value.find('"')? + 1;
            let second_quote = raw_value[first_quote..].find('"')? + first_quote;
            raw_value[first_quote..second_quote].to_string()
        } else {
            raw_value.to_string()
        };
        Some((key, value))
    }

    /// Evaluate JSON path operator (#> or #>>)
    fn eval_json_path(left: &Value, right: &Value, as_text: bool) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON path requires JSON or text operand".to_string(),
                ));
            }
        };

        let path = match right {
            Value::Text(p) => p.as_str(),
            _ => return Err(ExecutorError::TypeMismatch("JSON path must be text".to_string())),
        };

        let result = Self::extract_json_path(json_str, path);
        match result {
            Some(value) => {
                if as_text {
                    Ok(Value::Text(value.to_string()))
                } else {
                    Ok(Value::Json(value.to_string()))
                }
            }
            None => Ok(Value::Null),
        }
    }

    /// Extract value at JSON path
    fn extract_json_path(json_str: &str, path: &str) -> Option<String> {
        let path = path.trim();
        if !path.starts_with('{') || !path.ends_with('}') {
            if !path.starts_with('[') || !path.ends_with(']') {
                return None;
            }
            let content = &path[1..path.len() - 1];
            let keys: Vec<&str> = content.split(',').map(|s| s.trim().trim_matches('"')).collect();
            let mut current = json_str.to_string();
            for key in keys {
                current = Self::extract_json_field(&current, key)?;
            }
            return Some(current);
        }
        let content = &path[1..path.len() - 1];
        let keys: Vec<&str> = content.split('.').map(|s| s.trim().trim_matches('"')).collect();
        let mut current = json_str.to_string();
        for key in keys {
            if key.starts_with('[') && key.ends_with(']') {
                let idx_str = &key[1..key.len() - 1];
                if let Ok(idx) = idx_str.parse::<usize>() {
                    current = Self::extract_json_array_element(&current, idx)?;
                } else {
                    return None;
                }
            } else {
                current = Self::extract_json_field(&current, key)?;
            }
        }
        Some(current)
    }

    /// Evaluate JSON existence operator (?)
    fn eval_json_exists(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists requires JSON or text operand".to_string(),
                ));
            }
        };

        let key = match right {
            Value::Text(k) => k.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists key must be text".to_string(),
                ));
            }
        };

        Ok(Value::Bool(Self::extract_json_field(json_str, key).is_some()))
    }

    /// Evaluate JSON existence for any keys (?|)
    fn eval_json_exists_any(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists requires JSON or text operand".to_string(),
                ));
            }
        };

        let keys = match right {
            Value::Text(k) => k.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists keys must be text".to_string(),
                ));
            }
        };

        let keys = keys.trim();
        if !keys.starts_with('[') || !keys.ends_with(']') {
            return Err(ExecutorError::TypeMismatch(
                "JSON exists keys must be an array".to_string(),
            ));
        }

        let content = &keys[1..keys.len() - 1];
        if content.trim().is_empty() {
            return Ok(Value::Bool(false));
        }

        let key_list: Vec<&str> = content.split(',').map(|s| s.trim().trim_matches('"')).collect();
        for key in key_list {
            if Self::extract_json_field(json_str, key).is_some() {
                return Ok(Value::Bool(true));
            }
        }
        Ok(Value::Bool(false))
    }

    /// Evaluate JSON existence for all keys (?&)
    fn eval_json_exists_all(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists requires JSON or text operand".to_string(),
                ));
            }
        };

        let keys = match right {
            Value::Text(k) => k.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists keys must be text".to_string(),
                ));
            }
        };

        let keys = keys.trim();
        if !keys.starts_with('[') || !keys.ends_with(']') {
            return Err(ExecutorError::TypeMismatch(
                "JSON exists keys must be an array".to_string(),
            ));
        }

        let content = &keys[1..keys.len() - 1];
        if content.trim().is_empty() {
            return Ok(Value::Bool(false));
        }

        let key_list: Vec<&str> = content.split(',').map(|s| s.trim().trim_matches('"')).collect();
        for key in key_list {
            if Self::extract_json_field(json_str, key).is_none() {
                return Ok(Value::Bool(false));
            }
        }
        Ok(Value::Bool(true))
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
            (Value::Float(l), Value::Float(r)) => {
                Ok(Value::Bool(cmp_fn(l.partial_cmp(r).unwrap())))
            }
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

    /// Evaluate array contains operator (@>)
    fn eval_array_contains(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Array(left_arr), Value::Array(right_arr)) => {
                for elem in right_arr {
                    let mut found = false;
                    for item in left_arr {
                        if item == elem {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            (Value::Array(arr), elem) => {
                for item in arr {
                    if item == elem {
                        return Ok(Value::Bool(true));
                    }
                }
                Ok(Value::Bool(false))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Array contains (@>) requires array on left side".to_string(),
            )),
        }
    }

    /// Evaluate array contained by operator (<@)
    fn eval_array_contained_by(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
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
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Array contained by (<@) requires arrays on both sides".to_string(),
            )),
        }
    }

    /// Evaluate array overlaps operator (&&)
    fn eval_array_overlaps(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let (left_arr, right_arr) = match (left, right) {
            (Value::Array(l), Value::Array(r)) => (l, r),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "Array overlaps (&&) requires arrays on both sides".to_string(),
                ));
            }
        };

        for left_item in left_arr {
            for right_item in right_arr {
                if left_item == right_item {
                    return Ok(Value::Bool(true));
                }
            }
        }
        Ok(Value::Bool(false))
    }

    /// Evaluate array concat operator (||)
    fn eval_array_concat(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Array(l), Value::Array(r)) => {
                let mut result = l.clone();
                result.extend(r.clone());
                Ok(Value::Array(result))
            }
            (Value::Array(arr), elem) => {
                let mut result = arr.clone();
                result.push(elem.clone());
                Ok(Value::Array(result))
            }
            (elem, Value::Array(arr)) => {
                let mut result = vec![elem.clone()];
                result.extend(arr.clone());
                Ok(Value::Array(result))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Array concat (||) requires at least one array operand".to_string(),
            )),
        }
    }

    /// Evaluate array element access (arr[1])
    fn eval_array_access(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let arr = match left {
            Value::Array(arr) => arr,
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "Array element access requires array on left side".to_string(),
                ));
            }
        };

        let idx = match right {
            Value::Int(idx) => *idx,
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "Array index must be an integer".to_string(),
                ));
            }
        };

        if idx <= 0 {
            return Err(ExecutorError::InvalidArrayIndex("Array index must be >= 1".to_string()));
        }

        let idx = idx as usize;
        if idx > arr.len() {
            return Ok(Value::Null);
        }

        Ok(arr[idx - 1].clone())
    }

    /// Evaluate a function call
    pub fn eval_function(name: &str, args: Vec<Value>) -> Result<Value, ExecutorError> {
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
                if args[0] == args[1] { Ok(Value::Null) } else { Ok(args[0].clone()) }
            }
            "CONCAT" => {
                // Variadic function - concatenate all arguments (skip NULLs)
                let mut result = String::new();
                for arg in args {
                    match arg {
                        Value::Text(s) => result.push_str(&s),
                        Value::Int(i) => result.push_str(&i.to_string()),
                        Value::Null => continue,
                        _ => {
                            return Err(ExecutorError::TypeMismatch(
                                "CONCAT requires text or numeric values".to_string(),
                            ));
                        }
                    }
                }
                Ok(Value::Text(result))
            }
            "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecutorError::TypeMismatch(
                        "SUBSTRING takes 2 or 3 arguments".to_string(),
                    ));
                }
                let length = if args.len() == 3 { Some(args[2].clone()) } else { None };
                string_functions::StringFunctions::substring(
                    args[0].clone(),
                    args[1].clone(),
                    length,
                )
                .map_err(ExecutorError::TypeMismatch)
            }
            _ => Err(ExecutorError::FunctionNotFound(format!("Function '{}' not found", name))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};

    fn create_test_tuple() -> Tuple {
        [
            ("a".to_string(), Value::Int(10)),
            ("b".to_string(), Value::Text("hello".to_string())),
            ("c".to_string(), Value::Bool(true)),
            ("d".to_string(), Value::Null),
        ]
        .into()
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
    fn test_concat_two_strings() {
        let result = Eval::eval_function(
            "CONCAT",
            vec![Value::Text("hello".to_string()), Value::Text("world".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_concat_three_strings() {
        let result = Eval::eval_function(
            "CONCAT",
            vec![
                Value::Text("hello".to_string()),
                Value::Text(" ".to_string()),
                Value::Text("world".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello world".to_string()));
    }

    #[test]
    fn test_concat_with_int() {
        let result =
            Eval::eval_function("CONCAT", vec![Value::Text("Value: ".to_string()), Value::Int(42)])
                .unwrap();
        assert_eq!(result, Value::Text("Value: 42".to_string()));
    }

    #[test]
    fn test_concat_mixed_types() {
        let result = Eval::eval_function(
            "CONCAT",
            vec![Value::Text("SKU".to_string()), Value::Text(" - ".to_string()), Value::Int(123)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("SKU - 123".to_string()));
    }

    #[test]
    fn test_concat_with_null() {
        let result = Eval::eval_function(
            "CONCAT",
            vec![Value::Text("hello".to_string()), Value::Null, Value::Text("world".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_concat_all_nulls() {
        let result = Eval::eval_function("CONCAT", vec![Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_concat_empty_args() {
        let result = Eval::eval_function("CONCAT", vec![]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_concat_single_arg() {
        let result =
            Eval::eval_function("CONCAT", vec![Value::Text("single".to_string())]).unwrap();
        assert_eq!(result, Value::Text("single".to_string()));
    }

    #[test]
    fn test_concat_invalid_type() {
        let result = Eval::eval_function("CONCAT", vec![Value::Bool(true)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CONCAT requires text or numeric values"));
    }

    #[test]
    fn test_substring_two_args() {
        let result = Eval::eval_function(
            "SUBSTRING",
            vec![Value::Text("hello world".to_string()), Value::Int(1)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello world".to_string()));
    }

    #[test]
    fn test_substring_three_args() {
        let result = Eval::eval_function(
            "SUBSTRING",
            vec![Value::Text("hello world".to_string()), Value::Int(1), Value::Int(5)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_substring_invalid_args() {
        let result = Eval::eval_function("SUBSTRING", vec![Value::Text("hello".to_string())]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("SUBSTRING takes 2 or 3 arguments"));
    }

    #[test]
    fn test_substring_from_start() {
        let result = Eval::eval_function(
            "SUBSTRING",
            vec![Value::Text("hello world".to_string()), Value::Int(1), Value::Int(5)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_substring_middle() {
        let result = Eval::eval_function(
            "SUBSTRING",
            vec![Value::Text("hello world".to_string()), Value::Int(7), Value::Int(5)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("world".to_string()));
    }

    #[test]
    fn test_substring_without_length() {
        let result = Eval::eval_function(
            "SUBSTRING",
            vec![Value::Text("hello world".to_string()), Value::Int(7)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("world".to_string()));
    }

    #[test]
    fn test_upper_function() {
        let result = Eval::eval_function("UPPER", vec![Value::Text("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Text("HELLO".to_string()));
    }

    #[test]
    fn test_lower_function() {
        let result = Eval::eval_function("LOWER", vec![Value::Text("HELLO".to_string())]).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_length_function() {
        let result = Eval::eval_function("LENGTH", vec![Value::Text("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_coalesce_returns_first_non_null() {
        let result = Eval::eval_function(
            "COALESCE",
            vec![
                Value::Null,
                Value::Null,
                Value::Text("found".to_string()),
                Value::Text("ignored".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::Text("found".to_string()));
    }

    #[test]
    fn test_coalesce_all_nulls() {
        let result = Eval::eval_function("COALESCE", vec![Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_nullif_equal_returns_null() {
        let result = Eval::eval_function(
            "NULLIF",
            vec![Value::Text("same".to_string()), Value::Text("same".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_nullif_different_returns_first() {
        let result = Eval::eval_function(
            "NULLIF",
            vec![Value::Text("first".to_string()), Value::Text("second".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Text("first".to_string()));
    }

    #[test]
    fn test_array_contains_true() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let right = Value::Int(2);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayContains, &right).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_array_contains_false() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let right = Value::Int(5);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayContains, &right).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_array_contains_empty_array() {
        let left = Value::Array(vec![]);
        let right = Value::Int(1);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayContains, &right).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_array_contained_by() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result =
            Eval::eval_binary_op(&left, &BinaryOperator::ArrayContainedBy, &right).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_array_contained_by_false() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let right = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result =
            Eval::eval_binary_op(&left, &BinaryOperator::ArrayContainedBy, &right).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_array_overlaps_true() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Array(vec![Value::Int(2), Value::Int(3)]);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayOverlaps, &right).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_array_overlaps_false() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Array(vec![Value::Int(3), Value::Int(4)]);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayOverlaps, &right).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_array_concat_two_arrays() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Array(vec![Value::Int(3), Value::Int(4)]);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayConcat, &right).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)])
        );
    }

    #[test]
    fn test_array_concat_array_and_element() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Int(3);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayConcat, &right).unwrap();
        assert_eq!(result, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
    }

    #[test]
    fn test_array_concat_element_and_array() {
        let left = Value::Int(1);
        let right = Value::Array(vec![Value::Int(2), Value::Int(3)]);
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayConcat, &right).unwrap();
        assert_eq!(result, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
    }

    #[test]
    fn test_array_concat_with_strings() {
        let left = Value::Array(vec![Value::Text("a".to_string()), Value::Text("b".to_string())]);
        let right = Value::Text("c".to_string());
        let result = Eval::eval_binary_op(&left, &BinaryOperator::ArrayConcat, &right).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
                Value::Text("c".to_string())
            ])
        );
    }

    #[test]
    fn test_array_element_access_valid_index() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let idx = Value::Int(2);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx).unwrap();
        assert_eq!(result, Value::Int(20));
    }

    #[test]
    fn test_array_element_access_first_index() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let idx = Value::Int(1);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx).unwrap();
        assert_eq!(result, Value::Int(10));
    }

    #[test]
    fn test_array_element_access_last_index() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let idx = Value::Int(3);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx).unwrap();
        assert_eq!(result, Value::Int(30));
    }

    #[test]
    fn test_array_element_access_out_of_bounds() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let idx = Value::Int(5);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_array_element_access_zero_index() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let idx = Value::Int(0);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_element_access_negative_index() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let idx = Value::Int(-1);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_element_access_empty_array() {
        let arr = Value::Array(vec![]);
        let idx = Value::Int(1);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_array_element_access_string_array() {
        let arr = Value::Array(vec![
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
            Value::Text("cherry".to_string()),
        ]);
        let idx = Value::Int(2);
        let result = Eval::eval_binary_op(&arr, &BinaryOperator::ArrayAccess, &idx).unwrap();
        assert_eq!(result, Value::Text("banana".to_string()));
    }
}
