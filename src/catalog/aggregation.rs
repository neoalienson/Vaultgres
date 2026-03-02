use super::{predicate::PredicateEvaluator, TableSchema, Tuple, Value};
use crate::parser::ast::Expr;
use crate::transaction::TransactionManager;
use std::sync::Arc;

pub struct Aggregator;

impl Aggregator {
    pub fn execute(
        _table_name: &str,
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
            "COUNT" => Value::Int(values.len() as i64),
            "SUM" => {
                let sum: i64 = values
                    .iter()
                    .filter_map(|v| if let Value::Int(n) = v { Some(*n) } else { None })
                    .sum();
                Value::Int(sum)
            }
            "AVG" => {
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
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| &c.name == col_name)
                    .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                key.push(row[idx].clone());
            }
            groups.entry(key).or_default().push(row);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{ColumnDef, DataType};
    use crate::transaction::TupleHeader;

    fn create_test_data() -> (TableSchema, Vec<Tuple>, Arc<TransactionManager>) {
        let schema = TableSchema {
            name: "test".to_string(),
            columns: vec![
                ColumnDef::new("category".to_string(), DataType::Text),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        };

        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        txn_mgr.commit(txn.xid).unwrap();

        let tuples = vec![
            Tuple { header, data: vec![Value::Text("A".to_string()), Value::Int(10)] },
            Tuple { header, data: vec![Value::Text("B".to_string()), Value::Int(20)] },
            Tuple { header, data: vec![Value::Text("A".to_string()), Value::Int(30)] },
        ];

        (schema, tuples, txn_mgr)
    }

    #[test]
    fn test_count_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();

        let result =
            Aggregator::execute("test", "AGG:COUNT:*", None, &tuples, &schema, &txn_mgr).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Int(3));
    }

    #[test]
    fn test_sum_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();

        let result =
            Aggregator::execute("test", "AGG:SUM:value", None, &tuples, &schema, &txn_mgr).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Int(60));
    }

    #[test]
    fn test_avg_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();

        let result =
            Aggregator::execute("test", "AGG:AVG:value", None, &tuples, &schema, &txn_mgr).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Int(20));
    }

    #[test]
    fn test_min_max_aggregate() {
        let (schema, tuples, txn_mgr) = create_test_data();

        let result =
            Aggregator::execute("test", "AGG:MIN:value", None, &tuples, &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(10));

        let result =
            Aggregator::execute("test", "AGG:MAX:value", None, &tuples, &schema, &txn_mgr).unwrap();
        assert_eq!(result[0][0], Value::Int(30));
    }

    #[test]
    fn test_group_by() {
        let schema = TableSchema {
            name: "test".to_string(),
            columns: vec![
                ColumnDef::new("category".to_string(), DataType::Text),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        };

        let rows = vec![
            vec![Value::Text("A".to_string()), Value::Int(10)],
            vec![Value::Text("B".to_string()), Value::Int(20)],
            vec![Value::Text("A".to_string()), Value::Int(30)],
        ];

        let result = Aggregator::apply_group_by(
            rows,
            &["category".to_string()],
            &["category".to_string()],
            &schema,
        )
        .unwrap();

        assert_eq!(result.len(), 2);
    }
}
