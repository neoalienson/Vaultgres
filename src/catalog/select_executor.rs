use super::{TableSchema, Tuple, Value};
use crate::catalog::aggregation::Aggregator;
use crate::catalog::predicate::PredicateEvaluator;
use crate::parser::ast::{Expr, OrderByExpr};
use crate::transaction::TransactionManager;
use std::collections::HashSet;
use std::sync::Arc;

pub struct SelectExecutor;

impl SelectExecutor {
    pub fn execute(
        table: &str,
        columns: Vec<String>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<String>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
        tuples: &[Tuple],
        schema: &TableSchema,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<Vec<Vec<Value>>, String> {
        Self::execute_with_distinct(
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
        table: &str,
        distinct: bool,
        columns: Vec<String>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<String>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
        tuples: &[Tuple],
        schema: &TableSchema,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<Vec<Vec<Value>>, String> {
        if columns.len() == 1 && columns[0].starts_with("AGG:") {
            return Aggregator::execute(table, &columns[0], where_clause, tuples, schema, txn_mgr);
        }

        let snapshot = txn_mgr.get_snapshot();
        let mut results = Vec::new();

        for tuple in tuples {
            if !tuple.header.is_visible(&snapshot, txn_mgr) {
                continue;
            }

            if let Some(ref pred) = where_clause {
                if !PredicateEvaluator::evaluate(pred, &tuple.data, schema)? {
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

    fn matches_predicate(tuple: &Tuple, where_clause: &Option<Expr>, schema: &TableSchema) -> bool {
        where_clause
            .as_ref()
            .map(|pred| PredicateEvaluator::evaluate(pred, &tuple.data, schema).unwrap_or(false))
            .unwrap_or(true)
    }

    fn project_columns(
        tuple: &Tuple,
        columns: &[String],
        schema: &TableSchema,
    ) -> Result<Vec<Value>, String> {
        if columns.is_empty() || columns[0] == "*" {
            return Ok(tuple.data.clone());
        }

        columns
            .iter()
            .map(|col_name| {
                schema
                    .columns
                    .iter()
                    .position(|c| &c.name == col_name)
                    .map(|idx| tuple.data[idx].clone())
                    .ok_or_else(|| format!("Column '{}' not found", col_name))
            })
            .collect()
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
                if order_expr.ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
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
