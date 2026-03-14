use super::{Catalog, TableSchema, Tuple, Value, predicate::PredicateEvaluator};
use crate::parser::ast::Expr;
use crate::transaction::TransactionManager;
use std::sync::Arc;

pub struct Aggregator;

impl Aggregator {
    pub fn execute(
        catalog: &Catalog,
        _table_name: &str,
        agg_expr: &Expr,
        where_clause: Option<Expr>,
        tuples: &[Tuple],
        schema: &TableSchema,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let snapshot = txn_mgr.get_snapshot();

        let (func, arg_expr) = if let Expr::Aggregate { func, arg } = agg_expr {
            (func, arg)
        } else {
            return Err("Invalid aggregate expression".to_string());
        };

        let col_name =
            if let Expr::Column(name) = &**arg_expr { Some(name.as_str()) } else { None };

        let mut values = Vec::new();
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, txn_mgr) {
                if let Some(ref predicate) = where_clause {
                    let subquery_eval = |select: &crate::parser::ast::SelectStmt| {
                        super::select_executor::SelectExecutor::eval_scalar_subquery(
                            catalog, select,
                        )
                    };
                    let in_subquery_eval =
                        |select: &crate::parser::ast::SelectStmt, value: &Value| {
                            super::select_executor::SelectExecutor::eval_in_subquery(
                                catalog, select, value,
                            )
                        };
                    if !PredicateEvaluator::evaluate_with_in_subquery(
                        predicate,
                        &tuple.data,
                        schema,
                        &subquery_eval,
                        &in_subquery_eval,
                    )? {
                        continue;
                    }
                }

                if let crate::parser::ast::AggregateFunc::Count = func {
                    values.push(Value::Int(1));
                } else if let Some(col) = col_name {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == col)
                        .ok_or_else(|| format!("Column '{}' not found", col))?;
                    values.push(tuple.data[idx].clone());
                }
            }
        }

        let result = match func {
            crate::parser::ast::AggregateFunc::Count => Value::Int(values.len() as i64),
            crate::parser::ast::AggregateFunc::Sum => {
                let sum: i64 = values
                    .iter()
                    .filter_map(|v| if let Value::Int(n) = v { Some(*n) } else { None })
                    .sum();
                Value::Int(sum)
            }
            crate::parser::ast::AggregateFunc::Avg => {
                let nums: Vec<i64> = values
                    .iter()
                    .filter_map(|v| if let Value::Int(n) = v { Some(*n) } else { None })
                    .collect();
                if nums.is_empty() {
                    Value::Int(0)
                } else {
                    Value::Int(nums.iter().sum::<i64>() / nums.len() as i64)
                }
            }
            crate::parser::ast::AggregateFunc::Min => {
                values.iter().min().cloned().unwrap_or(Value::Int(0))
            }
            crate::parser::ast::AggregateFunc::Max => {
                values.iter().max().cloned().unwrap_or(Value::Int(0))
            }
        };

        Ok(vec![vec![result]])
    }

    pub fn apply_group_by(
        rows: Vec<Vec<Value>>,
        group_cols: &[Expr],
        select_cols: &[Expr],
        schema: &TableSchema,
    ) -> Result<Vec<Vec<Value>>, String> {
        use std::collections::HashMap;

        let mut groups: HashMap<Vec<Value>, Vec<Vec<Value>>> = HashMap::new();

        for row in rows {
            let mut key = Vec::new();
            for col_expr in group_cols {
                if let Expr::Column(col_name) = col_expr {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                    key.push(row[idx].clone());
                } else {
                    return Err(format!("Unsupported expression in GROUP BY: {:?}", col_expr));
                }
            }
            groups.entry(key).or_default().push(row);
        }

        let mut result = Vec::new();
        for (key, group_rows) in groups {
            let mut row = Vec::new();
            for col_expr in select_cols {
                if let Expr::Column(col_name) = col_expr {
                    // Check if the select column is one of the group by columns
                    if group_cols.iter().any(|g_expr| {
                        if let Expr::Column(g_name) = g_expr { g_name == col_name } else { false }
                    }) {
                        let idx = group_cols
                            .iter()
                            .position(|g_expr| {
                                if let Expr::Column(g_name) = g_expr {
                                    g_name == col_name
                                } else {
                                    false
                                }
                            })
                            .unwrap();
                        row.push(key[idx].clone());
                    } else {
                        row.push(Value::Int(group_rows.len() as i64)); // Placeholder for aggregates
                    }
                } else {
                    // This is where aggregates should be evaluated
                    row.push(Value::Int(group_rows.len() as i64)); // Placeholder for aggregates
                }
            }
            result.push(row);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType};
    use crate::transaction::TupleHeader;
    use std::collections::HashMap;

    fn create_test_data() -> (TableSchema, Vec<Tuple>, Arc<TransactionManager>) {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef::new("category".to_string(), DataType::Text),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        );

        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        txn_mgr.commit(txn.xid).unwrap();

        let tuples = vec![
            Tuple {
                header,
                data: vec![Value::Text("A".to_string()), Value::Int(10)],
                column_map: HashMap::new(),
            },
            Tuple {
                header,
                data: vec![Value::Text("B".to_string()), Value::Int(20)],
                column_map: HashMap::new(),
            },
            Tuple {
                header,
                data: vec![Value::Text("A".to_string()), Value::Int(30)],
                column_map: HashMap::new(),
            },
        ];

        (schema, tuples, txn_mgr)
    }

    #[test]
    fn test_count_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Count,
            arg: Box::new(Expr::Star),
        };

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Int(3));
    }

    #[test]
    fn test_sum_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        };

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Int(60));
    }

    #[test]
    fn test_avg_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Avg,
            arg: Box::new(Expr::Column("value".to_string())),
        };

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Int(20));
    }

    #[test]
    fn test_min_max_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr_min = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Min,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let agg_expr_max = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Max,
            arg: Box::new(Expr::Column("value".to_string())),
        };

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr_min, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(10));

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr_max, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(30));
    }

    #[test]
    fn test_group_by() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef::new("category".to_string(), DataType::Text),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        );

        let rows = vec![
            vec![Value::Text("A".to_string()), Value::Int(10)],
            vec![Value::Text("B".to_string()), Value::Int(20)],
            vec![Value::Text("A".to_string()), Value::Int(30)],
        ];

        let result = Aggregator::apply_group_by(
            rows,
            &[Expr::Column("category".to_string())],
            &[Expr::Column("category".to_string())],
            &schema,
        )
        .unwrap();

        assert_eq!(result.len(), 2);
    }

    // New tests for execute function error handling and edge cases

    #[test]
    fn test_execute_invalid_aggregate_expression() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Number(10); // Not an aggregate expression

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid aggregate expression");
    }

    #[test]
    fn test_execute_column_not_found() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Sum,
            arg: Box::new(Expr::Column("non_existent".to_string())),
        };

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Column 'non_existent' not found");
    }

    #[test]
    fn test_execute_empty_tuples() {
        let (schema, _tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Count,
            arg: Box::new(Expr::Star),
        };

        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &[], &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(0));

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &[], &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(0));

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Avg,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &[], &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(0));

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Min,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &[], &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(0));

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Max,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &[], &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(0));
    }

    #[test]
    fn test_execute_aggregate_with_null_values() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        );

        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        txn_mgr.commit(txn.xid).unwrap();

        let tuples = vec![
            Tuple { header, data: vec![Value::Int(1), Value::Int(10)], column_map: HashMap::new() },
            Tuple { header, data: vec![Value::Int(2), Value::Null], column_map: HashMap::new() },
            Tuple { header, data: vec![Value::Int(3), Value::Int(20)], column_map: HashMap::new() },
        ];

        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(30)); // NULL values should be ignored

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Avg,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(15)); // (10 + 20) / 2

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Min,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(10));

        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Max,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(20));
    }

    #[test]
    fn test_execute_count_with_where_clause() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Count,
            arg: Box::new(Expr::Star),
        };
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("category".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("A".to_string())),
        });

        let result = Aggregator::execute(
            &catalog,
            "test",
            &agg_expr,
            where_clause,
            &tuples,
            &schema,
            &txn_mgr,
        )
        .unwrap();

        assert_eq!(result[0][0], Value::Int(2)); // Two tuples have category 'A'
    }

    #[test]
    fn test_execute_sum_with_where_clause() {
        let (schema, tuples, txn_mgr) = create_test_data();
        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Sum,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("category".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("A".to_string())),
        });

        let result = Aggregator::execute(
            &catalog,
            "test",
            &agg_expr,
            where_clause,
            &tuples,
            &schema,
            &txn_mgr,
        )
        .unwrap();

        assert_eq!(result[0][0], Value::Int(40)); // 10 + 30
    }

    #[test]
    fn test_execute_avg_empty_numeric_values() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("value".to_string(), DataType::Text), // Not Int
            ],
        );

        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        txn_mgr.commit(txn.xid).unwrap();

        let tuples = vec![Tuple {
            header,
            data: vec![Value::Int(1), Value::Text("abc".to_string())],
            column_map: HashMap::new(),
        }];

        let catalog = Catalog::new();
        let agg_expr = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Avg,
            arg: Box::new(Expr::Column("value".to_string())),
        };
        let result =
            Aggregator::execute(&catalog, "test", &agg_expr, None, &tuples, &schema, &txn_mgr)
                .unwrap();
        assert_eq!(result[0][0], Value::Int(0)); // Should be 0 when no numeric values
    }
}
