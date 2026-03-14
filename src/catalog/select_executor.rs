use super::{Catalog, TableSchema, Tuple, Value};
use crate::catalog::aggregation::Aggregator;
use crate::catalog::predicate::PredicateEvaluator;
use crate::parser::ast::{Expr, OrderByExpr, SelectStmt};
use crate::transaction::TransactionManager;
use std::collections::HashSet;
use std::sync::Arc;

pub struct SelectExecutor;

impl SelectExecutor {
    pub fn execute(
        catalog: &Catalog,
        table: &str,
        columns: Vec<Expr>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
        tuples: &[Tuple],
        schema: &TableSchema,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<Vec<Vec<Value>>, String> {
        Self::execute_with_distinct(
            catalog,
            table,
            false,
            columns,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            tuples,
            schema,
            txn_mgr,
        )
    }

    pub fn execute_with_distinct(
        catalog: &Catalog,
        table: &str,
        distinct: bool,
        columns: Vec<Expr>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
        tuples: &[Tuple],
        schema: &TableSchema,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<Vec<Vec<Value>>, String> {
        if columns.len() == 1 {
            if let Some(Expr::Aggregate { .. }) = columns.first() {
                log::debug!("Taking Aggregator path: {:?}", columns[0]);
                return Aggregator::execute(
                    catalog,
                    table,
                    &columns[0],
                    where_clause,
                    tuples,
                    schema,
                    txn_mgr,
                );
            }
        }

        log::trace!("Taking normal SELECT path, columns={:?}", columns);

        let snapshot = txn_mgr.get_snapshot();
        let mut results = Vec::new();

        for tuple in tuples {
            if !tuple.header.is_visible(&snapshot, txn_mgr) {
                continue;
            }

            if let Some(ref pred) = where_clause {
                log::trace!("Evaluating WHERE clause with subquery support");
                let subquery_eval =
                    |select: &SelectStmt| Self::eval_scalar_subquery(catalog, select);
                let in_subquery_eval = |select: &SelectStmt, value: &Value| {
                    Self::eval_in_subquery(catalog, select, value)
                };
                if !PredicateEvaluator::evaluate_with_in_subquery(
                    pred,
                    &tuple.data,
                    schema,
                    &subquery_eval,
                    &in_subquery_eval,
                )? {
                    continue;
                }
            }

            results.push(Self::project_columns(tuple, &columns, schema)?);
        }

        if let Some(group_cols) = group_by {
            results = Aggregator::apply_group_by(results, &group_cols, &columns, schema)?;
            if let Some(having_expr) = having {
                results.retain(|row| {
                    PredicateEvaluator::evaluate_having(&having_expr, row).unwrap_or(false)
                });
            }
        }

        if let Some(order_by_exprs) = order_by {
            Self::apply_order_by(&mut results, &order_by_exprs, schema)?;
        }

        results = Self::apply_limit_offset(results, limit, offset);

        if distinct {
            Self::apply_distinct(&mut results);
        }

        Ok(results)
    }

    pub fn eval_scalar_subquery(catalog: &Catalog, select: &SelectStmt) -> Result<Value, String> {
        log::debug!("eval_scalar_subquery: table={}", select.from);
        let table = &select.from;
        let result = catalog.select(
            table,
            false,
            select.columns.clone(),
            select.where_clause.clone(),
            select.group_by.clone(),
            select.having.clone(),
            select.order_by.clone(),
            select.limit,
            select.offset,
        )?;

        if result.is_empty() || result[0].is_empty() {
            return Err("Subquery returned no results".to_string());
        }

        Ok(result[0][0].clone())
    }

    pub fn eval_in_subquery(
        catalog: &Catalog,
        select: &SelectStmt,
        value: &Value,
    ) -> Result<bool, String> {
        log::debug!("eval_in_subquery: table={}, checking value={:?}", select.from, value);
        let table = &select.from;
        let result = catalog.select(
            table,
            false,
            select.columns.clone(),
            select.where_clause.clone(),
            select.group_by.clone(),
            select.having.clone(),
            select.order_by.clone(),
            select.limit,
            select.offset,
        )?;

        // Check if value exists in any row's first column
        for row in result {
            if !row.is_empty() && &row[0] == value {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn matches_predicate(tuple: &Tuple, where_clause: &Option<Expr>, schema: &TableSchema) -> bool {
        where_clause
            .as_ref()
            .map(|pred| PredicateEvaluator::evaluate(pred, &tuple.data, schema).unwrap_or(false))
            .unwrap_or(true)
    }

    fn project_columns(
        tuple: &Tuple,
        columns: &[Expr],
        schema: &TableSchema,
    ) -> Result<Vec<Value>, String> {
        if columns.is_empty() || matches!(columns.first(), Some(Expr::Star)) {
            return Ok(tuple.data.clone());
        }

        columns.iter().map(|expr| Self::eval_expr(expr, tuple, schema)).collect()
    }

    fn eval_expr(expr: &Expr, tuple: &Tuple, schema: &TableSchema) -> Result<Value, String> {
        match expr {
            Expr::Column(name) => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| format!("Column '{}' not found", name))?;
                Ok(tuple.data[idx].clone())
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::FunctionCall { name, args } => {
                let mut evaluated_args = Vec::new();
                for arg in args {
                    evaluated_args.push(Self::eval_expr(arg, tuple, schema)?);
                }
                Self::eval_function(name, evaluated_args)
            }
            _ => Err(format!("Unsupported expression {:?}", expr)),
        }
    }

    fn eval_function(name: &str, args: Vec<Value>) -> Result<Value, String> {
        use crate::catalog::string_functions;
        match name.to_uppercase().as_str() {
            "UPPER" => {
                if args.len() != 1 {
                    return Err("UPPER takes one argument".to_string());
                }
                string_functions::StringFunctions::upper(args[0].clone())
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err("LOWER takes one argument".to_string());
                }
                string_functions::StringFunctions::lower(args[0].clone())
            }
            "LENGTH" => {
                if args.len() != 1 {
                    return Err("LENGTH takes one argument".to_string());
                }
                string_functions::StringFunctions::length(args[0].clone())
            }
            "CONCAT" => string_functions::StringFunctions::concat(args),
            "SUBSTRING" => {
                if args.len() == 2 {
                    string_functions::StringFunctions::substring(
                        args[0].clone(),
                        args[1].clone(),
                        None,
                    )
                } else if args.len() == 3 {
                    string_functions::StringFunctions::substring(
                        args[0].clone(),
                        args[1].clone(),
                        Some(args[2].clone()),
                    )
                } else {
                    Err("SUBSTRING takes 2 or 3 arguments".to_string())
                }
            }
            _ => Err(format!("Function '{}' not found", name)),
        }
    }

    fn apply_order_by(
        results: &mut [Vec<Value>],
        order_by_exprs: &[OrderByExpr],
        schema: &TableSchema,
    ) -> Result<(), String> {
        for order_expr in order_by_exprs.iter().rev() {
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name == order_expr.column)
                .ok_or_else(|| format!("Column '{}' not found", order_expr.column))?;

            results.sort_by(|a, b| {
                let cmp = a[col_idx].cmp(&b[col_idx]);
                if order_expr.ascending { cmp } else { cmp.reverse() }
            });
        }
        Ok(())
    }

    fn apply_limit_offset(
        results: Vec<Vec<Value>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Vec<Vec<Value>> {
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(results.len());
        results.into_iter().skip(start).take(end.saturating_sub(start)).collect()
    }

    fn apply_distinct(results: &mut Vec<Vec<Value>>) {
        let mut seen = HashSet::new();
        results.retain(|row| seen.insert(row.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- apply_limit_offset tests ---

    #[test]
    fn test_apply_limit_offset_no_limit_no_offset() {
        let data = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
        let result = SelectExecutor::apply_limit_offset(data.clone(), None, None);
        assert_eq!(result, data);
    }

    #[test]
    fn test_apply_limit_offset_with_limit() {
        let data = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];
        let result = SelectExecutor::apply_limit_offset(data, Some(2), None);
        assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    }

    #[test]
    fn test_apply_limit_offset_with_offset() {
        let data = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];
        let result = SelectExecutor::apply_limit_offset(data, None, Some(1));
        assert_eq!(result, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    }

    #[test]
    fn test_apply_limit_offset_with_limit_and_offset() {
        let data = vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ];
        let result = SelectExecutor::apply_limit_offset(data, Some(2), Some(1));
        assert_eq!(result, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    }

    #[test]
    fn test_apply_limit_offset_limit_greater_than_len() {
        let data = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
        let result = SelectExecutor::apply_limit_offset(data.clone(), Some(5), None);
        assert_eq!(result, data);
    }

    #[test]
    fn test_apply_limit_offset_offset_greater_than_len() {
        let data = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
        let result = SelectExecutor::apply_limit_offset(data, None, Some(5));
        assert!(result.is_empty());
    }

    // --- apply_distinct tests ---

    #[test]
    fn test_apply_distinct_unique_rows() {
        let mut data = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
        let expected = data.clone();
        SelectExecutor::apply_distinct(&mut data);
        assert_eq!(data, expected);
    }

    #[test]
    fn test_apply_distinct_with_duplicates() {
        let mut data = vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(2)],
        ];
        SelectExecutor::apply_distinct(&mut data);
        assert_eq!(data, vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]]);
    }

    #[test]
    fn test_apply_distinct_empty() {
        let mut data: Vec<Vec<Value>> = vec![];
        SelectExecutor::apply_distinct(&mut data);
        assert!(data.is_empty());
    }
}
