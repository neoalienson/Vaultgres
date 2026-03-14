use super::{TableSchema, Tuple, Value};
use crate::catalog::predicate::PredicateEvaluator;
use crate::parser::ast::{DataType, Expr};
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
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(predicate) = where_clause {
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            Self::apply_assignments(tuple, assignments, schema)?;
            updated += 1;
        }
        Ok(updated)
    }

    fn apply_assignments(
        tuple: &mut Tuple,
        assignments: &[(String, Expr)],
        schema: &TableSchema,
    ) -> Result<(), String> {
        for (col_name, expr) in assignments {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| format!("Column '{}' not found", col_name))?;

            let value = match expr {
                Expr::Number(n) => Value::Int(*n),
                Expr::String(s) => Value::Text(s.clone()),
                _ => return Err("Invalid value expression".to_string()),
            };

            Self::validate_type(&schema.columns[idx].data_type, &value, col_name)?;
            tuple.data[idx] = value;
        }
        Ok(())
    }

    fn validate_type(data_type: &DataType, value: &Value, col_name: &str) -> Result<(), String> {
        match (data_type, value) {
            (DataType::Int, Value::Int(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Varchar(_), Value::Text(_)) => Ok(()),
            _ => Err(format!("Type mismatch for column '{}'", col_name)),
        }
    }

    pub fn delete(
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
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
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
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};
    use crate::transaction::{Snapshot, TransactionManager, TupleHeader};
    use std::collections::HashMap;

    // Mock PredicateEvaluator for testing purposes
    // In a real scenario, you might have a more sophisticated mock or use test data that
    // always makes PredicateEvaluator::evaluate return true for simplicity if the focus
    // is not on predicate evaluation itself.
    // Here, we'll implement a basic mock that only evaluates simple equality expressions.
    struct MockPredicateEvaluator;

    impl MockPredicateEvaluator {
        fn evaluate(
            expr: &Expr,
            tuple_data: &[Value],
            schema: &TableSchema,
        ) -> Result<bool, String> {
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    let left_val = match left.as_ref() {
                        Expr::Column(col_name) => {
                            let idx = schema
                                .columns
                                .iter()
                                .position(|c| &c.name == col_name)
                                .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                            tuple_data[idx].clone()
                        }
                        Expr::Number(n) => Value::Int(*n),
                        Expr::String(s) => Value::Text(s.clone()),
                        _ => return Err("Unsupported expression in mock predicate".to_string()),
                    };
                    let right_val = match right.as_ref() {
                        Expr::Column(col_name) => {
                            let idx = schema
                                .columns
                                .iter()
                                .position(|c| &c.name == col_name)
                                .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                            tuple_data[idx].clone()
                        }
                        Expr::Number(n) => Value::Int(*n),
                        Expr::String(s) => Value::Text(s.clone()),
                        _ => return Err("Unsupported expression in mock predicate".to_string()),
                    };

                    match op {
                        BinaryOperator::Equals => Ok(left_val == right_val),
                        BinaryOperator::NotEquals => Ok(left_val != right_val),
                        _ => Err("Unsupported operator in mock predicate".to_string()),
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

            UpdateDeleteExecutor::apply_assignments(tuple, assignments, schema)?;
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

    // Helper to create a test schema
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

    // Helper to create a test tuple
    fn create_test_tuple(xmin: u64, id: i64, name: &str, age: i64) -> Tuple {
        let mut tuple =
            Tuple { header: TupleHeader::new(xmin), data: vec![], column_map: HashMap::new() };
        tuple.add_value("id".to_string(), Value::Int(id));
        tuple.add_value("name".to_string(), Value::Text(name.to_string()));
        tuple.add_value("age".to_string(), Value::Int(age));
        tuple
    }

    // --- validate_type tests ---
    #[test]
    fn test_validate_type_success() {
        assert!(
            UpdateDeleteExecutor::validate_type(&DataType::Int, &Value::Int(10), "col").is_ok()
        );
        assert!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Text,
                &Value::Text("hello".to_string()),
                "col"
            )
            .is_ok()
        );
        assert!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Varchar(10),
                &Value::Text("short".to_string()),
                "col"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_validate_type_mismatch() {
        assert!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Int,
                &Value::Text("hello".to_string()),
                "col"
            )
            .is_err()
        );
        assert_eq!(
            UpdateDeleteExecutor::validate_type(
                &DataType::Int,
                &Value::Text("hello".to_string()),
                "col"
            )
            .unwrap_err(),
            "Type mismatch for column 'col'"
        );

        assert!(
            UpdateDeleteExecutor::validate_type(&DataType::Text, &Value::Int(10), "col").is_err()
        );
    }

    // --- apply_assignments tests ---
    #[test]
    fn test_apply_assignments_success() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![
            ("name".to_string(), Expr::String("Bob".to_string())),
            ("age".to_string(), Expr::Number(31)),
        ];

        assert!(UpdateDeleteExecutor::apply_assignments(&mut tuple, &assignments, &schema).is_ok());
        assert_eq!(tuple.get_value("name"), Some(Value::Text("Bob".to_string())));
        assert_eq!(tuple.get_value("age"), Some(Value::Int(31)));
    }

    #[test]
    fn test_apply_assignments_column_not_found() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("non_existent".to_string(), Expr::Number(100))];

        let result = UpdateDeleteExecutor::apply_assignments(&mut tuple, &assignments, &schema);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Column 'non_existent' not found");
    }

    #[test]
    fn test_apply_assignments_type_mismatch() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("name".to_string(), Expr::Number(123))]; // name is TEXT

        let result = UpdateDeleteExecutor::apply_assignments(&mut tuple, &assignments, &schema);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Type mismatch for column 'name'");
    }

    #[test]
    fn test_apply_assignments_invalid_expression() {
        let schema = create_test_schema();
        let mut tuple = create_test_tuple(1, 1, "Alice", 30);
        let assignments = vec![("age".to_string(), Expr::Column("other_col".to_string()))]; // Unsupported Expr

        let result = UpdateDeleteExecutor::apply_assignments(&mut tuple, &assignments, &schema);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid value expression");
    }

    // --- update tests ---
    #[test]
    fn test_update_single_tuple() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        txn_mgr.commit(txn.xid).unwrap();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let snapshot = Snapshot::new(txn.xid, txn.xid + 1, vec![]); // Mock snapshot

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &None, // No WHERE clause
            &schema,
            &snapshot,
            &txn_mgr,
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

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
        )
        .unwrap();

        assert_eq!(updated_count, 2);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(40)));
        assert_eq!(tuples[1].get_value("age"), Some(Value::Int(25))); // Bob not updated
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

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &txn_mgr,
        )
        .unwrap();

        assert_eq!(updated_count, 0);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(30))); // Not updated
    }

    #[test]
    fn test_update_tuple_not_visible() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        let assignments = vec![("age".to_string(), Expr::Number(31))];
        let snapshot = Snapshot::new(0, txn.xid, vec![txn.xid]); // Snapshot before txn commits

        let updated_count = update_with_mock_evaluator(
            &mut tuples,
            &assignments,
            &None,
            &schema,
            &snapshot,
            &txn_mgr,
        )
        .unwrap();

        assert_eq!(updated_count, 0);
        assert_eq!(tuples[0].get_value("age"), Some(Value::Int(30))); // Not updated
    }

    // --- delete tests ---
    #[test]
    fn test_delete_single_tuple() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let xid = txn_mgr.begin().xid;
        txn_mgr.commit(xid).unwrap(); // Commit the transaction that created the tuple
        let mut tuples = vec![create_test_tuple(xid, 1, "Alice", 30)];
        let snapshot = Snapshot::new(xid, xid + 1, vec![]);

        let deleted_count = delete_with_mock_evaluator(
            &mut tuples,
            &None, // No WHERE clause
            &schema,
            &snapshot,
            &txn_mgr,
            xid + 1, // XID for the deleting transaction
        )
        .unwrap();

        assert_eq!(deleted_count, 1);
        // Verify that the tuple is marked as deleted (xmax is set)
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

        // Make tuples visible
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
        assert_ne!(tuples[0].header.xmax, 0); // Alice 1 deleted
        assert_eq!(tuples[1].header.xmax, 0); // Bob not deleted
        assert_ne!(tuples[2].header.xmax, 0); // Alice 2 deleted
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
        assert_eq!(tuples[0].header.xmax, 0); // Not deleted
    }

    #[test]
    fn test_delete_tuple_not_visible() {
        let schema = create_test_schema();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let mut tuples = vec![create_test_tuple(txn.xid, 1, "Alice", 30)];
        // Tuple's xmin is txn.xid, but its transaction is not committed.
        // So a snapshot with xmax txn.xid (default) won't see it.
        let snapshot = Snapshot::new(0, txn.xid, vec![txn.xid]); // Before tuple's creation txn was committed
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
        assert_eq!(tuples[0].header.xmax, 0); // Not deleted
    }
}
