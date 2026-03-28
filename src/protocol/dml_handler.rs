//! DML statement execution
//!
//! Handles SELECT, INSERT, UPDATE, DELETE statements including
//! JOIN queries and CTE (WITH) queries.

use crate::catalog::predicate::PredicateEvaluator;
use crate::catalog::{Catalog, TableSchema, Value};
use crate::parser::ast::JoinType;
use crate::parser::ast::{SelectStmt, Statement};
use crate::planner::planner::Planner;
use std::sync::Arc;

use super::projection::{
    build_combined_schema, build_result_set, extract_column_names, project_columns,
};

pub enum DmlResult {
    CommandComplete(String),
    ResultSet(super::result_set::ResultSet),
}

pub fn execute_dml(catalog: Arc<Catalog>, stmt: Statement) -> Result<DmlResult, String> {
    match stmt {
        Statement::Describe(desc) => {
            if let Some(schema) = catalog.get_table(&desc.table) {
                let cols: Vec<String> = schema
                    .columns
                    .iter()
                    .map(|c| format!("{}: {:?}", c.name, c.data_type))
                    .collect();
                Ok(DmlResult::CommandComplete(format!("DESCRIBE\n{}", cols.join("\n"))))
            } else {
                Err(format!("Table '{}' does not exist", desc.table))
            }
        }
        Statement::Insert(insert) => {
            if insert.batch_values.is_empty() {
                catalog.insert(&insert.table, &insert.columns, insert.values)?;
                Ok(DmlResult::CommandComplete("INSERT 0 1".to_string()))
            } else {
                let mut all_rows = vec![insert.values];
                all_rows.extend(insert.batch_values);
                let count = catalog.batch_insert(&insert.table, &insert.columns, all_rows)?;
                Ok(DmlResult::CommandComplete(format!("INSERT 0 {}", count)))
            }
        }
        Statement::Select(select_stmt) => execute_select(catalog, select_stmt),
        Statement::With(with_stmt) => {
            log::debug!(
                "Executing WITH: recursive={}, ctes={}, query_from={}",
                with_stmt.recursive,
                with_stmt.ctes.len(),
                with_stmt.query.from
            );

            let planner = Planner::new_with_catalog(catalog.clone());
            let mut plan = planner.plan_with_cte(&with_stmt).map_err(|e| format!("{:?}", e))?;

            let mut rows: Vec<Vec<Value>> = Vec::new();
            let mut output_column_names: Option<Vec<String>> = None;

            loop {
                match plan.next() {
                    Ok(Some(tuple_hashmap)) => {
                        let mut row = Vec::new();

                        if output_column_names.is_none() {
                            output_column_names = Some(tuple_hashmap.keys().cloned().collect());
                        }

                        if let Some(ref col_names) = output_column_names {
                            for col_name in col_names {
                                row.push(
                                    tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null),
                                );
                            }
                        }
                        rows.push(row);
                    }
                    Ok(None) => break,
                    Err(e) => return Err(format!("{:?}", e)),
                }
            }

            let column_names = output_column_names.unwrap_or_default();
            let result_set = build_result_set(&*catalog, &column_names, rows)?;
            Ok(DmlResult::ResultSet(result_set))
        }
        Statement::Update(update) => {
            let count = catalog.update(&update.table, update.assignments, update.where_clause)?;
            Ok(DmlResult::CommandComplete(format!("UPDATE {}", count)))
        }
        Statement::Delete(delete) => {
            let count = catalog.delete(&delete.table, delete.where_clause)?;
            Ok(DmlResult::CommandComplete(format!("DELETE {}", count)))
        }
        _ => Ok(DmlResult::CommandComplete("SELECT 0".to_string())),
    }
}

fn execute_select(catalog: Arc<Catalog>, select_stmt: SelectStmt) -> Result<DmlResult, String> {
    log::debug!(
        "Executing SELECT: from={}, joins={}, has_where={}",
        select_stmt.from,
        select_stmt.joins.len(),
        select_stmt.where_clause.is_some()
    );

    if let Some((mv_data, column_names)) =
        catalog.get_materialized_view_with_columns(&select_stmt.from)
    {
        log::debug!(
            "Found materialized view: {} with {} columns",
            select_stmt.from,
            column_names.len()
        );
        let result_set = build_result_set(&*catalog, &column_names, mv_data)?;
        return Ok(DmlResult::ResultSet(result_set));
    }

    let has_aggregates = select_stmt.columns.iter().any(|col| contains_aggregate(col))
        || select_stmt.group_by.as_ref().is_some_and(|gb| !gb.is_empty());

    if !select_stmt.joins.is_empty() && !has_aggregates {
        return execute_join_query(catalog, select_stmt);
    }

    let planner = Planner::new_with_catalog(catalog.clone());
    let mut plan = planner.plan(&select_stmt).map_err(|e| format!("{:?}", e))?;

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut output_column_names: Option<Vec<String>> = None;

    loop {
        match plan.next() {
            Ok(Some(tuple_hashmap)) => {
                let mut row = Vec::new();

                if output_column_names.is_none() {
                    output_column_names = Some(tuple_hashmap.keys().cloned().collect());
                }

                if let Some(ref col_names) = output_column_names {
                    for col_name in col_names {
                        row.push(tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null));
                    }
                }
                rows.push(row);
            }
            Ok(None) => break,
            Err(e) => return Err(format!("{:?}", e)),
        }
    }

    let column_names = output_column_names.unwrap_or_default();
    log::trace!("planner execution returned {} rows", rows.len());

    let result_set = build_result_set(&*catalog, &column_names, rows)?;
    Ok(DmlResult::ResultSet(result_set))
}

fn contains_aggregate(expr: &crate::parser::Expr) -> bool {
    use crate::parser::Expr;
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Alias { expr, .. } => contains_aggregate(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(contains_aggregate),
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Case { conditions, else_expr } => {
            conditions.iter().any(|(w, t)| contains_aggregate(w) || contains_aggregate(t))
                || else_expr.as_ref().map_or(false, |e| contains_aggregate(e))
        }
        _ => false,
    }
}

fn execute_join_query(catalog: Arc<Catalog>, select: SelectStmt) -> Result<DmlResult, String> {
    log::info!("[JOIN] Executing JOIN query. From: {}, Joins: {}", select.from, select.joins.len());

    let left_table = &select.from;
    let left_schema =
        catalog.get_table(left_table).ok_or_else(|| format!("Table '{}' not found", left_table))?;
    let left_alias = select.table_alias.as_ref().unwrap_or(left_table);

    let mut all_schemas = vec![(left_alias.clone(), left_schema.clone())];
    for join in &select.joins {
        let schema = catalog
            .get_table(&join.table)
            .ok_or_else(|| format!("Table '{}' not found", join.table))?;
        let alias = join.alias.as_ref().unwrap_or(&join.table);
        all_schemas.push((alias.clone(), schema.clone()));
    }

    log::info!(
        "[JOIN] Schema map: {:?}",
        all_schemas
            .iter()
            .map(|(a, s)| (a.clone(), s.name.clone()))
            .collect::<Vec<(String, String)>>()
    );

    let snapshot = catalog.txn_mgr.get_snapshot();
    let data = catalog.data.read().unwrap();
    let left_tuples =
        data.get(left_table).ok_or_else(|| format!("Table '{}' has no data", left_table))?;

    let mut results = Vec::new();
    for left_tuple in left_tuples {
        if !left_tuple.header.is_visible(&snapshot, &catalog.txn_mgr) {
            continue;
        }

        let mut current_row = left_tuple.data.clone();
        let mut matched = true;

        for (join_idx, join) in select.joins.iter().enumerate() {
            let right_tuples = data
                .get(&join.table)
                .ok_or_else(|| format!("Table '{}' has no data", join.table))?;

            let mut join_matched = false;
            for right_tuple in right_tuples {
                if !right_tuple.header.is_visible(&snapshot, &catalog.txn_mgr) {
                    continue;
                }

                let combined = [current_row.clone(), right_tuple.data.clone()].concat();
                let combined_schema = build_combined_schema(&all_schemas[..=join_idx + 1]);
                if PredicateEvaluator::evaluate(
                    &join.on,
                    &combined,
                    &combined_schema,
                    &catalog.enum_types,
                )? {
                    current_row.extend_from_slice(&right_tuple.data);
                    join_matched = true;
                    break;
                }
            }

            if !join_matched && join.join_type == JoinType::Inner {
                matched = false;
                break;
            }
        }

        if matched {
            if let Some(ref where_clause) = select.where_clause {
                let combined_schema = build_combined_schema(&all_schemas);
                if !PredicateEvaluator::evaluate(
                    where_clause,
                    &current_row,
                    &combined_schema,
                    &catalog.enum_types,
                )? {
                    continue;
                }
            }
            results.push(current_row);
        }
    }

    let column_names = extract_column_names(&select.columns, &all_schemas)?;
    let projected = project_columns(&*catalog, &results, &select.columns, &all_schemas)?;
    let result_set = build_result_set(&*catalog, &column_names, projected)?;
    Ok(DmlResult::ResultSet(result_set))
}
