use super::*;
use crate::parser::ast::{ColumnDef, DataType, Expr, BinaryOperator, OrderByExpr};
    
    #[test]
    fn test_create_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        assert!(catalog.create_table("users".to_string(), columns).is_ok());
        assert!(catalog.get_table("users").is_some());
    }
    
    #[test]
    fn test_create_duplicate_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns.clone()).unwrap();
        assert!(catalog.create_table("users".to_string(), columns).is_err());
    }
    
    #[test]
    fn test_drop_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        assert!(catalog.drop_table("users", false).is_ok());
        assert!(catalog.get_table("users").is_none());
    }
    
    #[test]
    fn test_drop_nonexistent_table() {
        let catalog = Catalog::new();
        assert!(catalog.drop_table("users", false).is_err());
        assert!(catalog.drop_table("users", true).is_ok());
    }
    
    #[test]
    fn test_insert() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
        assert!(catalog.insert("users", values).is_ok());
        assert_eq!(catalog.row_count("users"), 1);
    }
    
    #[test]
    fn test_insert_wrong_column_count() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
        assert!(catalog.insert("users", values).is_err());
    }
    
    #[test]
    fn test_insert_type_mismatch() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        let values = vec![Expr::String("not a number".to_string())];
        assert!(catalog.insert("users", values).is_err());
    }
    
    #[test]
    fn test_insert_multiple_rows() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())]).unwrap();
        
        assert_eq!(catalog.row_count("users"), 3);
    }
    
    #[test]
    fn test_select_all() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        
        let rows = catalog.select("users", false, vec!["*".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
    }
    
    #[test]
    fn test_select_specific_columns() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        
        let rows = catalog.select("users", false, vec!["id".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
    }
    
    #[test]
    fn test_select_nonexistent_table() {
        let catalog = Catalog::new();
        let result = catalog.select("nonexistent", false, vec!["*".to_string()], None, None, None, None, None, None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_select_empty_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("empty".to_string(), columns).unwrap();
        let rows = catalog.select("empty", false, vec!["*".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 0);
    }
    
    #[test]
    fn test_select_with_where() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(2)),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(2));
        assert_eq!(rows[0][1], Value::Int(200));
    }
    
    #[test]
    fn test_select_with_not_equals() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::NotEquals,
            right: Box::new(Expr::Number(2)),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_less_than() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(25)),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_greater_than() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(15)),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_less_than_or_equal() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(Expr::Number(20)),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_greater_than_or_equal() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(20)),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_update() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        
        let updated = catalog.update("data", vec![("value".to_string(), Expr::Number(999))], None).unwrap();
        assert_eq!(updated, 2);
    }
    
    #[test]
    fn test_update_nonexistent_table() {
        let catalog = Catalog::new();
        let result = catalog.update("nonexistent", vec![("col".to_string(), Expr::Number(1))], None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_update_with_where() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        });
        
        let updated = catalog.update("data", vec![("value".to_string(), Expr::Number(999))], where_clause).unwrap();
        assert_eq!(updated, 1);
    }
    
    #[test]
    fn test_delete() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let deleted = catalog.delete("data", None).unwrap();
        assert_eq!(deleted, 3);
    }
    
    #[test]
    fn test_delete_empty_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("empty".to_string(), columns).unwrap();
        let deleted = catalog.delete("empty", None).unwrap();
        assert_eq!(deleted, 0);
    }
    
    #[test]
    fn test_delete_with_where() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(2)),
        });
        
        let deleted = catalog.delete("data", where_clause).unwrap();
        assert_eq!(deleted, 1);
    }
    
    #[test]
    fn test_select_with_order_by_asc() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        
        let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: true }]);
        let rows = catalog.select("data", false, vec!["*".to_string()], None, None, None, order_by, None, None).unwrap();
        
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[1][0], Value::Int(2));
        assert_eq!(rows[2][0], Value::Int(3));
    }
    
    #[test]
    fn test_select_with_order_by_desc() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        
        let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: false }]);
        let rows = catalog.select("data", false, vec!["*".to_string()], None, None, None, order_by, None, None).unwrap();
        
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Int(3));
        assert_eq!(rows[1][0], Value::Int(2));
        assert_eq!(rows[2][0], Value::Int(1));
    }

    #[test]
    fn test_select_with_limit() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        catalog.insert("data", vec![Expr::Number(4)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["*".to_string()], None, None, None, None, Some(2), None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_offset() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        catalog.insert("data", vec![Expr::Number(4)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["*".to_string()], None, None, None, None, None, Some(2)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(3));
    }
    
    #[test]
    fn test_select_with_limit_and_offset() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        catalog.insert("data", vec![Expr::Number(4)]).unwrap();
        catalog.insert("data", vec![Expr::Number(5)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["*".to_string()], None, None, None, None, Some(2), Some(1)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(2));
        assert_eq!(rows[1][0], Value::Int(3));
    }

    
    #[test]
    fn test_aggregate_count() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["AGG:COUNT:*".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(3));
    }
    
    #[test]
    fn test_aggregate_sum() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["AGG:SUM:value".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(60));
    }
    
    #[test]
    fn test_aggregate_avg() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["AGG:AVG:value".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(20));
    }
    
    #[test]
    fn test_aggregate_min_max() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(50)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let rows = catalog.select("data", false, vec!["AGG:MIN:value".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows[0][0], Value::Int(10));
        
        let rows = catalog.select("data", false, vec!["AGG:MAX:value".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows[0][0], Value::Int(50));
    }

    
    #[test]
    fn test_where_with_and() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3), Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(1)),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("value".to_string())),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(30)),
            }),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(2));
    }
    
    #[test]
    fn test_where_with_or() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(1)),
            }),
            op: BinaryOperator::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(3)),
            }),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_group_by() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "category".to_string(), data_type: DataType::Text },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::String("B".to_string()), Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(30)]).unwrap();
        catalog.insert("data", vec![Expr::String("B".to_string()), Expr::Number(40)]).unwrap();
        
        let group_by = Some(vec!["category".to_string()]);
        let rows = catalog.select("data", false, vec!["category".to_string()], None, group_by, None, None, None, None).unwrap();
        
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_having_clause() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "category".to_string(), data_type: DataType::Text },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::String("B".to_string()), Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(30)]).unwrap();
        catalog.insert("data", vec![Expr::String("C".to_string()), Expr::Number(5)]).unwrap();
        
        let group_by = Some(vec!["category".to_string()]);
        let having = Some(Expr::BinaryOp {
            left: Box::new(Expr::Number(2)),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(1)),
        });
        
        let rows = catalog.select("data", false, vec!["category".to_string()], None, group_by, having, None, None, None).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_distinct() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "category".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::String("A".to_string())]).unwrap();
        catalog.insert("data", vec![Expr::String("B".to_string())]).unwrap();
        catalog.insert("data", vec![Expr::String("A".to_string())]).unwrap();
        catalog.insert("data", vec![Expr::String("B".to_string())]).unwrap();
        
        let rows = catalog.select("data", true, vec!["category".to_string()], None, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_like_operator() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::String("hello world".to_string())]).unwrap();
        catalog.insert("data", vec![Expr::String("goodbye".to_string())]).unwrap();
        catalog.insert("data", vec![Expr::String("hello there".to_string())]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("%hello%".to_string())),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_in_operator() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        catalog.insert("data", vec![Expr::Number(4)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![Expr::Number(1), Expr::Number(3)])),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_between_operator() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(5)]).unwrap();
        catalog.insert("data", vec![Expr::Number(15)]).unwrap();
        catalog.insert("data", vec![Expr::Number(25)]).unwrap();
        catalog.insert("data", vec![Expr::Number(35)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Between,
            right: Box::new(Expr::List(vec![Expr::Number(10), Expr::Number(30)])),
        });
        
        let rows = catalog.select("data", false, vec!["*".to_string()], where_clause, None, None, None, None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
