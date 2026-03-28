use super::{Catalog, TableSchema, Tuple, Value};
use crate::catalog::predicate::PredicateEvaluator;
use crate::catalog::update_apply;
use crate::parser::ast::Expr;
use crate::transaction::{Snapshot, TransactionManager};
use std::sync::Arc;

pub struct UpdateDeleteExecutor;

impl UpdateDeleteExecutor {
    pub fn update(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        catalog: &Catalog,
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(
                    predicate,
                    &tuple.data,
                    schema,
                    &catalog.enum_types,
                )? {
                    continue;
                }
            }

            update_apply::apply_assignments(tuple, assignments, schema, catalog)?;
            updated += 1;
        }
        Ok(updated)
    }

    pub fn update_with_tuples(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        catalog: &Catalog,
        subquery_tuples: &[Tuple],
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(
                    predicate,
                    &tuple.data,
                    schema,
                    &catalog.enum_types,
                )? {
                    continue;
                }
            }

            update_apply::apply_assignments_with_tuples(
                tuple,
                assignments,
                schema,
                catalog,
                subquery_tuples,
                snapshot,
            )?;
            updated += 1;
        }
        Ok(updated)
    }

    pub fn delete(
        tuples: &mut [Tuple],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        xid: u64,
        catalog: &Catalog,
    ) -> Result<usize, String> {
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(
                    predicate,
                    &tuple.data,
                    schema,
                    &catalog.enum_types,
                )? {
                    continue;
                }
            }

            tuple.header.delete(xid);
            deleted += 1;
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::TableSchema;
    use crate::catalog::update_apply as ua;
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};
    use crate::transaction::{Snapshot, TransactionManager, TupleHeader};
    use std::collections::HashMap;
    use std::sync::Arc;

    struct MockPredicateEvaluator;

    impl MockPredicateEvaluator {
        fn eval_expr(
            expr: &Expr,
            tuple_data: &[Value],
            schema: &TableSchema,
        ) -> Result<Value, String> {
            match expr {
                Expr::Column(name) => {
                    let lookup_name = if let Some(dot_pos) = name.find('.') {
                        &name[dot_pos + 1..]
                    } else {
                        name
                    };
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| &c.name == lookup_name)
                        .ok_or_else(|| format!("Column '{}' not found", name))?;
                    Ok(tuple_data[idx].clone())
                }
                Expr::Number(n) => Ok(Value::Int(*n)),
                Expr::String(s) => Ok(Value::Text(s.clone())),
                Expr::Null => Ok(Value::Null),
                Expr::BinaryOp { left, op, right } => {
                    let l = Self::eval_expr(left, tuple_data, schema)?;
                    let r = Self::eval_expr(right, tuple_data, schema)?;
                    crate::executor::expr_evaluator::eval_binary_op(&l, op, &r)
                }
                Expr::UnaryOp { op, expr } => {
                    let val = Self::eval_expr(expr, tuple_data, schema)?;
                    crate::executor::expr_evaluator::eval_unary_op(op, &val)
                }
                _ => Err(format!("Unsupported expression in mock predicate: {:?}", expr)),
            }
        }

        fn evaluate(
            expr: &Expr,
            tuple_data: &[Value],
            schema: &TableSchema,
        ) -> Result<bool, String> {
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    let left_val = Self::eval_expr(left, tuple_data, schema)?;
                    let right_val = Self::eval_expr(right, tuple_data, schema)?;
                    match op {
                        BinaryOperator::Equals => Ok(left_val == right_val),
                        BinaryOperator::NotEquals => Ok(left_val != right_val),
                        BinaryOperator::GreaterThan => {
                            let result = crate::executor::expr_evaluator::eval_binary_op(
                                &left_val, op, &right_val,
                            )?;
                            match result {
                                Value::Bool(b) => Ok(b),
                                _ => Err("Comparison must return bool".to_string()),
                            }
                        }
                        BinaryOperator::LessThan => {
                            let result = crate::executor::expr_evaluator::eval_binary_op(
                                &left_val, op, &right_val,
                            )?;
                            match result {
                                Value::Bool(b) => Ok(b),
                                _ => Err("Comparison must return bool".to_string()),
                            }
                        }
                        _ => Err(format!("Unsupported operator in mock predicate: {:?}", op)),
                    }
                }
                _ => Err("Unsupported expression in mock predicate".to_string()),
            }
        }
    }

    fn update_with_mock_evaluator(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        catalog: &Catalog,
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !MockPredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            ua::apply_assignments(tuple, assignments, schema, catalog)?;
            updated += 1;
        }
        Ok(updated)
    }

    fn delete_with_mock_evaluator(
        tuples: &mut [Tuple],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        xid: u64,
    ) -> Result<usize, String> {
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !MockPredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            tuple.header.delete(xid);
            deleted += 1;
        }
        Ok(deleted)
    }

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("age".to_string(), DataType::Int),
            ],
        )
    }

    fn create_test_tuple(xmin: u64, id: i64, name: &str, age: i64) -> Tuple {
        let mut tuple =
            Tuple { header: TupleHeader::new(xmin), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(id));
        tuple.add_value("name".to_string(), Value::Text(name.to_string()));
        tuple.add_value("age".to_string(), Value::Int(age));
        tuple
    }

    #[test]
    fn test_update_single_tuple() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        txn_mgr.commit(txn.xid).unwrap();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let snapshot = Snapshot::new(txn.xid, txn.xid + 1, vec![]);

        let catalog = Catalog::new();
        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &None,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 1);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(31)));
    }

    #[test]
    fn test_update_multiple_tuples_with_where() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![
            create_test_tuple(xid_creator, 1, "Alice", 30),
            create_test_tuple(xid_creator, 2, "Bob", 25),
            create_test_tuple(xid_creator, 3, "Alice", 35),
        ];
        let assignments = vec![("age".to_string(), Expr::Number(40))];
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Alice".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let catalog = Catalog::new();

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 2);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(40)));
        assert_eq!(tuples[1].get_value("age"), Some(Value::Int(25)));
        assert_eq!(tuples[2].get_value("age"), Some(Value::Int(40)));
    }

    #[test]
    fn test_update_no_matching_tuples() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![create_test_tuple(xid_creator, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Bob".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let catalog = Catalog::new();

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 0);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(30)));
    }

    #[test]
    fn test_update_tuple_not_visible() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let snapshot = Snapshot::new(0, txn.xid, vec![txn.xid]);
        let catalog = Catalog::new();

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &None,
            &schema,
            &snapshot,
            &txn_mgr,
            &catalog,
        )
        .unwrap();

        assert_eq!(updated_count, 0);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(30)));
    }

    #[test]
    fn test_delete_single_tuple() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid = txn_mgr.begin().xid;
        txn_mgr.commit(xid).unwrap();
        let mut tuples = vec![create_test_tuple(xid, 1, "Alice", 30)];
        let snapshot = Snapshot::new(xid, xid + 1, vec![]);

        let deleted_count =
            delete_with_mock_evaluator(&mut tuples, &None, &schema, &snapshot, &txn_mgr, xid + 1)
                .unwrap();

        assert_eq!(deleted_count, 1);
        assert_ne!(tuples[0].header.xmax, 0);
    }

    #[test]
    fn test_delete_multiple_tuples_with_where() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![
            create_test_tuple(xid_creator, 1, "Alice", 30),
            create_test_tuple(xid_creator, 2, "Bob", 25),
            create_test_tuple(xid_creator, 3, "Alice", 35),
        ];

        for tuple in tuples.iter_mut() {
            tuple.header.xmin = xid_creator;
        }

        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Alice".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let xid_deleter = txn_mgr.begin().xid;

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            xid_deleter,
        )
        .unwrap();

        assert_eq!(deleted_count, 2);
        assert_ne!(tuples[0].header.xmax, 0);
        assert_eq!(tuples[1].header.xmax, 0);
        assert_ne!(tuples[2].header.xmax, 0);
    }

    #[test]
    fn test_delete_no_matching_tuples() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid_creator = txn_mgr.begin().xid;
        txn_mgr.commit(xid_creator).unwrap();
        let mut tuples = vec![create_test_tuple(xid_creator, 1, "Alice", 30)];

        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("Bob".to_string())),
        });
        let snapshot = Snapshot::new(xid_creator, xid_creator + 1, vec![]);
        let xid_deleter = txn_mgr.begin().xid;

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
            xid_deleter,
        )
        .unwrap();

        assert_eq!(deleted_count, 0);
        assert_eq!(tuples[0].header.xmax, 0);
    }

    #[test]
    fn test_delete_tuple_not_visible() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let snapshot = Snapshot::new(0, txn.xid, vec![txn.xid]);
        let xid_deleter = txn_mgr.begin().xid;

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &None,
            &schema,
            &snapshot,
            &txn_mgr,
            xid_deleter,
        )
        .unwrap();

        assert_eq!(deleted_count, 0);
        assert_eq!(tuples[0].header.xmax, 0);
    }
}
