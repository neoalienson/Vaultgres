#[cfg(test)]
mod tests {
    use crate::catalog::Value;
    use crate::executor::plpgsql::PlPgSqlInterpreter;
    use crate::executor::plpgsql::evaluator::ExprEvaluator;
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

mod evaluator_tests {
    use crate::catalog::Value;
    use crate::executor::plpgsql::evaluator::ExprEvaluator;
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
    use std::collections::HashMap;

    #[test]
    fn test_eval_number() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval(&Expr::Number(123)).unwrap(), Value::Int(123));
        assert_eq!(evaluator.eval(&Expr::Number(0)).unwrap(), Value::Int(0));
        assert_eq!(evaluator.eval(&Expr::Number(-45)).unwrap(), Value::Int(-45));
    }

    #[test]
    fn test_eval_string() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(
            evaluator.eval(&Expr::String("hello".to_string())).unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_eval_column() {
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), Value::Int(10));
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval(&Expr::Column("x".to_string())).unwrap(), Value::Int(10));
    }

    #[test]
    fn test_eval_column_not_found() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let err = evaluator.eval(&Expr::Column("y".to_string())).unwrap_err();
        assert_eq!(err, "Variable 'y' not found");
    }

    #[test]
    fn test_eval_list() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let list_expr = Expr::List(vec![Expr::Number(1), Expr::String("a".to_string())]);
        assert_eq!(
            evaluator.eval(&list_expr).unwrap(),
            Value::Array(vec![Value::Int(1), Value::Text("a".to_string())])
        );
    }

    #[test]
    fn test_eval_list_empty() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let list_expr = Expr::List(vec![]);
        assert_eq!(evaluator.eval(&list_expr).unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_eval_unary_op_not_int() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let expr = Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(Expr::Number(0)) };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));

        let expr = Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(Expr::Number(1)) };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_eval_unary_op_not_int_as_bool() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let expr = Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(Expr::Number(0)) }; // 0 is false
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));

        let expr = Expr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(Expr::Number(1)) }; // 1 is true
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_eval_unary_op_not_unsupported_type() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::String("test".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "NOT requires integer or boolean");
    }

    #[test]
    fn test_eval_binary_op_int_arithmetic() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let add = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(3)),
        };
        assert_eq!(evaluator.eval(&add).unwrap(), Value::Int(8));

        let sub = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(3)),
        };
        assert_eq!(evaluator.eval(&sub).unwrap(), Value::Int(2));

        let mul = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(3)),
        };
        assert_eq!(evaluator.eval(&mul).unwrap(), Value::Int(15));

        let div = Expr::BinaryOp {
            left: Box::new(Expr::Number(6)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(3)),
        };
        assert_eq!(evaluator.eval(&div).unwrap(), Value::Int(2));

        let modulo = Expr::BinaryOp {
            left: Box::new(Expr::Number(7)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(3)),
        };
        assert_eq!(evaluator.eval(&modulo).unwrap(), Value::Int(1));
    }

    #[test]
    fn test_eval_binary_op_int_division_by_zero() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let div = Expr::BinaryOp {
            left: Box::new(Expr::Number(6)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(0)),
        };
        let err = evaluator.eval(&div).unwrap_err();
        assert_eq!(err, "Division by zero");

        let modulo = Expr::BinaryOp {
            left: Box::new(Expr::Number(7)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(0)),
        };
        let err = evaluator.eval(&modulo).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_eval_binary_op_int_comparison() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let eq = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&eq).unwrap(), Value::Bool(true));

        let gt = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(3)),
        };
        assert_eq!(evaluator.eval(&gt).unwrap(), Value::Bool(true));

        let lt = Expr::BinaryOp {
            left: Box::new(Expr::Number(3)),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&lt).unwrap(), Value::Bool(true));

        let ge = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&ge).unwrap(), Value::Bool(true));

        let le = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&le).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_binary_op_int_logical() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let and = Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::And,
            right: Box::new(Expr::Number(1)),
        };
        assert_eq!(evaluator.eval(&and).unwrap(), Value::Bool(true));

        let or = Expr::BinaryOp {
            left: Box::new(Expr::Number(0)),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Number(1)),
        };
        assert_eq!(evaluator.eval(&or).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_float_literal() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval(&Expr::Float(123.45)).unwrap(), Value::Float(123.45));
    }

    #[test]
    fn test_eval_binary_op_float_arithmetic() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let add = Expr::BinaryOp {
            left: Box::new(Expr::Float(5.0)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Float(3.0)),
        };
        assert!((evaluator.eval(&add).unwrap().as_float().unwrap() - 8.0).abs() < f64::EPSILON);

        let sub = Expr::BinaryOp {
            left: Box::new(Expr::Float(5.0)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Float(3.0)),
        };
        assert!((evaluator.eval(&sub).unwrap().as_float().unwrap() - 2.0).abs() < f64::EPSILON);

        let mul = Expr::BinaryOp {
            left: Box::new(Expr::Float(5.0)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Float(3.0)),
        };
        assert!((evaluator.eval(&mul).unwrap().as_float().unwrap() - 15.0).abs() < f64::EPSILON);

        let div = Expr::BinaryOp {
            left: Box::new(Expr::Float(6.0)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Float(3.0)),
        };
        assert!((evaluator.eval(&div).unwrap().as_float().unwrap() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_eval_binary_op_float_division_by_zero() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let div = Expr::BinaryOp {
            left: Box::new(Expr::Float(6.0)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Float(0.0)),
        };
        let err = evaluator.eval(&div).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_eval_binary_op_float_comparison() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let eq = Expr::BinaryOp {
            left: Box::new(Expr::Float(5.0)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Float(5.0)),
        };
        assert_eq!(evaluator.eval(&eq).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_binary_op_bool_logical() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let and = Expr::BinaryOp {
            left: Box::new(Expr::Number(1)), // using number for bool
            op: BinaryOperator::And,
            right: Box::new(Expr::Number(1)),
        };
        assert_eq!(evaluator.eval(&and).unwrap(), Value::Bool(true));

        let or = Expr::BinaryOp {
            left: Box::new(Expr::Number(0)), // using number for bool
            op: BinaryOperator::Or,
            right: Box::new(Expr::Number(1)),
        };
        assert_eq!(evaluator.eval(&or).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_binary_op_string_concat() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let concat = Expr::BinaryOp {
            left: Box::new(Expr::String("hello".to_string())),
            op: BinaryOperator::StringConcat,
            right: Box::new(Expr::String(" world".to_string())),
        };
        assert_eq!(evaluator.eval(&concat).unwrap(), Value::Text("hello world".to_string()));
    }

    #[test]
    fn test_eval_binary_op_string_like() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let like = Expr::BinaryOp {
            left: Box::new(Expr::String("hello world".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("h%o world".to_string())),
        };
        assert_eq!(evaluator.eval(&like).unwrap(), Value::Bool(true));

        let not_like = Expr::BinaryOp {
            left: Box::new(Expr::String("hello world".to_string())),
            op: BinaryOperator::Like,
            right: Box::new(Expr::String("x%".to_string())),
        };
        assert_eq!(evaluator.eval(&not_like).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_eval_binary_op_string_ilike() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let ilike = Expr::BinaryOp {
            left: Box::new(Expr::String("Hello World".to_string())),
            op: BinaryOperator::ILike,
            right: Box::new(Expr::String("h%o world".to_string())),
        };
        assert_eq!(evaluator.eval(&ilike).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_binary_op_type_mismatch() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::String("a".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Type mismatch in binary operation");
    }

    #[test]
    fn test_eval_unsupported_expr_type() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        // Use an Expr variant that is not explicitly handled in ExprEvaluator::eval
        // For example, Expr::Star is not handled in the match statement
        let expr = Expr::Star;
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Unsupported expression");
    }

    // eval_string tests
    #[test]
    fn test_eval_string_no_placeholders() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval_string("hello").unwrap(), "hello");
    }

    #[test]
    fn test_eval_string_single_placeholder() {
        let mut vars = HashMap::new();
        vars.insert("name".to_string(), Value::Text("world".to_string()));
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval_string("hello $name").unwrap(), "hello world");
    }

    #[test]
    fn test_eval_string_multiple_placeholders() {
        let mut vars = HashMap::new();
        vars.insert("name".to_string(), Value::Text("world".to_string()));
        vars.insert("num".to_string(), Value::Int(123));
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(
            evaluator.eval_string("hello $name, number $num").unwrap(),
            "hello world, number 123"
        );
    }

    #[test]
    fn test_eval_string_numeric_placeholder() {
        let mut vars = HashMap::new();
        vars.insert("num".to_string(), Value::Int(123));
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval_string("Value: $num").unwrap(), "Value: 123");
    }

    #[test]
    fn test_eval_string_bool_placeholder() {
        let mut vars = HashMap::new();
        vars.insert("flag".to_string(), Value::Bool(true));
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval_string("Flag is $flag").unwrap(), "Flag is true");
    }

    #[test]
    fn test_eval_string_null_placeholder() {
        let mut vars = HashMap::new();
        vars.insert("val".to_string(), Value::Null);
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval_string("Value is $val").unwrap(), "Value is NULL");
    }

    #[test]
    fn test_eval_string_non_existent_placeholder() {
        let vars = HashMap::new();
        let evaluator = ExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval_string("hello $name").unwrap(), "hello $name");
    }

    // is_true tests
    #[test]
    fn test_is_true_int() {
        assert!(ExprEvaluator::is_true(&Value::Int(1)));
        assert!(!ExprEvaluator::is_true(&Value::Int(0)));
        assert!(ExprEvaluator::is_true(&Value::Int(-1)));
    }

    #[test]
    fn test_is_true_bool() {
        assert!(ExprEvaluator::is_true(&Value::Bool(true)));
        assert!(!ExprEvaluator::is_true(&Value::Bool(false)));
    }

    #[test]
    fn test_is_true_other_types() {
        assert!(!ExprEvaluator::is_true(&Value::Text("hello".to_string())));
        assert!(!ExprEvaluator::is_true(&Value::Float(1.0)));
        assert!(!ExprEvaluator::is_true(&Value::Array(vec![])));
        assert!(!ExprEvaluator::is_true(&Value::Null));
    }
}
