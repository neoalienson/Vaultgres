use crate::parser::ast::Expr;
use crate::transaction::TransactionManager;
use super::{Value, TableSchema, Tuple, predicate::PredicateEvaluator};
use std::sync::Arc;

pub struct Aggregator;

impl Aggregator {
    pub fn execute(
        table_name: &str,
        agg_spec: &str,
        where_clause: Option<Expr>,
        tuples: &[Tuple],
        schema: &TableSchema,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let snapshot = txn_mgr.get_snapshot();
        
        let parts: Vec<&str> = agg_spec.split(':').collect();
        if parts.len() < 2 {
            return Err("Invalid aggregate specification".to_string());
        }
        
        let func = parts[1];
        let col_name = if parts.len() > 2 { Some(parts[2]) } else { None };
        
        let mut values = Vec::new();
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, txn_mgr) {
                if let Some(ref predicate) = where_clause {
                    if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                        continue;
                    }
                }
                
                if func == "COUNT" {
                    values.push(Value::Int(1));
                } else if let Some(col) = col_name {
                    let idx = schema.columns.iter().position(|c| c.name == col)
                        .ok_or_else(|| format!("Column '{}' not found", col))?;
                    values.push(tuple.data[idx].clone());
                }
            }
        }
        
        let result = match func {
            "COUNT" => Value::Int(values.len() as i64),
            "SUM" => {
                let sum: i64 = values.iter().filter_map(|v| {
                    if let Value::Int(n) = v { Some(*n) } else { None }
                }).sum();
                Value::Int(sum)
            }
            "AVG" => {
                let nums: Vec<i64> = values.iter().filter_map(|v| {
                    if let Value::Int(n) = v { Some(*n) } else { None }
                }).collect();
                if nums.is_empty() {
                    Value::Int(0)
                } else {
                    Value::Int(nums.iter().sum::<i64>() / nums.len() as i64)
                }
            }
            "MIN" => values.iter().min().cloned().unwrap_or(Value::Int(0)),
            "MAX" => values.iter().max().cloned().unwrap_or(Value::Int(0)),
            _ => return Err(format!("Unknown aggregate function: {}", func)),
        };
        
        Ok(vec![vec![result]])
    }

    pub fn apply_group_by(
        rows: Vec<Vec<Value>>,
        group_cols: &[String],
        select_cols: &[String],
        schema: &TableSchema,
    ) -> Result<Vec<Vec<Value>>, String> {
        use std::collections::HashMap;
        
        let mut groups: HashMap<Vec<Value>, Vec<Vec<Value>>> = HashMap::new();
        
        for row in rows {
            let mut key = Vec::new();
            for col_name in group_cols {
                let idx = schema.columns.iter().position(|c| &c.name == col_name)
                    .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                key.push(row[idx].clone());
            }
            groups.entry(key).or_insert_with(Vec::new).push(row);
        }
        
        let mut result = Vec::new();
        for (key, group_rows) in groups {
            let mut row = Vec::new();
            for col_name in select_cols {
                if group_cols.contains(col_name) {
                    let idx = group_cols.iter().position(|c| c == col_name).unwrap();
                    row.push(key[idx].clone());
                } else {
                    row.push(Value::Int(group_rows.len() as i64));
                }
            }
            result.push(row);
        }
        
        Ok(result)
    }
}
