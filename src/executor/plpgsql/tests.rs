#[cfg(test)]
mod tests {
    use crate::catalog::Value;
    use crate::executor::plpgsql::PlPgSqlInterpreter;
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
    use crate::parser::plpgsql_ast::{PlPgSqlFunction, PlPgSqlStmt};
    use std::collections::HashMap;

    #[test]
    fn test_exception_handling() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::ExceptionBlock {
                try_stmts: vec![PlPgSqlStmt::Raise { message: "error occurred".to_string() }],
                exception_var: "err".to_string(),
                catch_stmts: vec![PlPgSqlStmt::Return {
                    value: Some(Expr::Column("err".to_string())),
                }],
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Text("error occurred".to_string()));
    }

    #[test]
    fn test_dynamic_sql() {
        let mut interp = PlPgSqlInterpreter::new().with_query_executor(|_| Ok(vec![]));

        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![
                PlPgSqlStmt::Execute { query: "SELECT 1".to_string() },
                PlPgSqlStmt::Return { value: Some(Expr::Number(42)) },
            ],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_for_query() {
        let mut interp = PlPgSqlInterpreter::new().with_query_executor(|_| {
            let mut row1 = HashMap::new();
            row1.insert("id".to_string(), Value::Int(1));
            let mut row2 = HashMap::new();
            row2.insert("id".to_string(), Value::Int(2));
            Ok(vec![row1, row2])
        });

        let func = PlPgSqlFunction {
            declarations: vec![PlPgSqlStmt::Declare {
                name: "sum".to_string(),
                data_type: "INT".to_string(),
                default: Some(Expr::Number(0)),
            }],
            body: vec![
                PlPgSqlStmt::ForQuery {
                    var: "rec".to_string(),
                    query: "SELECT id FROM t".to_string(),
                    body: vec![PlPgSqlStmt::Assign {
                        target: "sum".to_string(),
                        value: Expr::BinaryOp {
                            left: Box::new(Expr::Column("sum".to_string())),
                            op: BinaryOperator::Add,
                            right: Box::new(Expr::Column("id".to_string())),
                        },
                    }],
                },
                PlPgSqlStmt::Return { value: Some(Expr::Column("sum".to_string())) },
            ],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_not_operator() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::Number(0)),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_string_concat() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::String("hello".to_string())),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(Expr::String(" world".to_string())),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Text("hello world".to_string()));
    }

    #[test]
    fn test_raise() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Raise { message: "custom error".to_string() }],
        };

        let result = interp.execute(&func, vec![]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "custom error");
    }

    #[test]
    fn test_perform() {
        let mut interp = PlPgSqlInterpreter::new().with_query_executor(|_| Ok(vec![]));

        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![
                PlPgSqlStmt::Perform { query: "SELECT 1".to_string() },
                PlPgSqlStmt::Return { value: Some(Expr::Number(42)) },
            ],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_bool_type() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Number(5)),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Number(3)),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_like_operator() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::String("hello world".to_string())),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::String("hello%".to_string())),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_ilike_operator() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::String("HELLO WORLD".to_string())),
                    op: BinaryOperator::ILike,
                    right: Box::new(Expr::String("hello%".to_string())),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_foreach_loop() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![
                PlPgSqlStmt::Declare {
                    name: "arr".to_string(),
                    data_type: "ARRAY".to_string(),
                    default: Some(Expr::List(vec![
                        Expr::Number(1),
                        Expr::Number(2),
                        Expr::Number(3),
                    ])),
                },
                PlPgSqlStmt::Declare {
                    name: "sum".to_string(),
                    data_type: "INT".to_string(),
                    default: Some(Expr::Number(0)),
                },
            ],
            body: vec![
                PlPgSqlStmt::ForEach {
                    var: "elem".to_string(),
                    array: Expr::Column("arr".to_string()),
                    body: vec![PlPgSqlStmt::Assign {
                        target: "sum".to_string(),
                        value: Expr::BinaryOp {
                            left: Box::new(Expr::Column("sum".to_string())),
                            op: BinaryOperator::Add,
                            right: Box::new(Expr::Column("elem".to_string())),
                        },
                    }],
                },
                PlPgSqlStmt::Return { value: Some(Expr::Column("sum".to_string())) },
            ],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(6));
    }
}
