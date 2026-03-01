#[cfg(test)]
mod tests {
    use crate::catalog::{Function, FunctionLanguage, FunctionRegistry, Parameter, Value};
    use crate::executor::{
        BuiltinFunctions, CorrelatedExecutor, PlPgSqlInterpreter, RecursiveCTEExecutor,
    };
    use crate::parser::ast::{BinaryOperator, Expr};
    use crate::parser::plpgsql_ast::{PlPgSqlFunction, PlPgSqlStmt};

    #[test]
    fn test_function_empty_body() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction { declarations: vec![], body: vec![] };
        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_function_no_return() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Assign { target: "x".to_string(), value: Expr::Number(42) }],
        };
        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_undefined_variable() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return { value: Some(Expr::Column("undefined".to_string())) }],
        };
        let result = interp.execute(&func, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_if() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::If {
                condition: Expr::Number(1),
                then_stmts: vec![PlPgSqlStmt::If {
                    condition: Expr::Number(1),
                    then_stmts: vec![PlPgSqlStmt::Return { value: Some(Expr::Number(100)) }],
                    else_stmts: vec![],
                }],
                else_stmts: vec![],
            }],
        };
        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(100));
    }

    #[test]
    fn test_for_loop_empty_range() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![PlPgSqlStmt::Declare {
                name: "count".to_string(),
                data_type: "INT".to_string(),
                default: Some(Expr::Number(0)),
            }],
            body: vec![
                PlPgSqlStmt::For {
                    var: "i".to_string(),
                    start: Expr::Number(5),
                    end: Expr::Number(3),
                    body: vec![PlPgSqlStmt::Assign {
                        target: "count".to_string(),
                        value: Expr::Number(1),
                    }],
                },
                PlPgSqlStmt::Return { value: Some(Expr::Column("count".to_string())) },
            ],
        };
        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_recursive_cte_max_iterations() {
        let base = vec![vec![Value::Int(1)]];

        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            if working.is_empty() {
                return Ok(vec![]);
            }
            let mut results = Vec::new();
            for row in working {
                if let Value::Int(n) = row[0] {
                    if n < 1000 {
                        results.push(vec![Value::Int(n + 1)]);
                    }
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert!(result.len() > 100);
    }

    #[test]
    fn test_correlated_exists_empty_outer() {
        let outer: Vec<Vec<Value>> = vec![];
        let subquery_fn =
            |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Int(1)]]) };

        let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_correlated_in_null_value() {
        let outer = vec![vec![Value::Null]];
        let subquery_fn =
            |_: &[Value]| -> Result<Vec<Vec<Value>>, String> { Ok(vec![vec![Value::Null]]) };

        let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_builtin_upper_empty_string() {
        let result = BuiltinFunctions::execute("upper", vec![Value::Text("".to_string())]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_builtin_length_empty() {
        let result =
            BuiltinFunctions::execute("length", vec![Value::Text("".to_string())]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_builtin_power_large_exponent() {
        let result =
            BuiltinFunctions::execute("power", vec![Value::Int(2), Value::Int(10)]).unwrap();
        assert_eq!(result, Value::Int(1024));
    }

    #[test]
    fn test_builtin_unknown_function() {
        let result = BuiltinFunctions::execute("unknown", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_function_registry_duplicate_overload() {
        let mut registry = FunctionRegistry::new();
        let func = Function {
            name: "test".to_string(),
            parameters: vec![Parameter {
                name: "a".to_string(),
                data_type: "INT".to_string(),
                default: None,
            }],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: "SELECT $1".to_string(),
            is_variadic: false,
        };

        registry.register(func.clone()).unwrap();
        registry.register(func).unwrap();

        let resolved = registry.resolve("test", &["INT".to_string()]);
        assert!(resolved.is_some());
    }

    #[test]
    fn test_while_loop_never_executes() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![PlPgSqlStmt::Declare {
                name: "result".to_string(),
                data_type: "INT".to_string(),
                default: Some(Expr::Number(0)),
            }],
            body: vec![
                PlPgSqlStmt::While {
                    condition: Expr::Number(0),
                    body: vec![PlPgSqlStmt::Assign {
                        target: "result".to_string(),
                        value: Expr::Number(100),
                    }],
                },
                PlPgSqlStmt::Return { value: Some(Expr::Column("result".to_string())) },
            ],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_recursive_cte_duplicate_detection() {
        let base = vec![vec![Value::Int(1)]];

        let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
            let mut results = Vec::new();
            for row in working {
                if let Value::Int(n) = row[0] {
                    if n == 1 {
                        results.push(vec![Value::Int(1)]);
                    }
                }
            }
            Ok(results)
        };

        let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_comparison_operators() {
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
    fn test_exception_no_error() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::ExceptionBlock {
                try_stmts: vec![PlPgSqlStmt::Return { value: Some(Expr::Number(42)) }],
                exception_var: "err".to_string(),
                catch_stmts: vec![PlPgSqlStmt::Return { value: Some(Expr::Number(0)) }],
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_raise_error() {
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
    fn test_for_query_empty_results() {
        let mut interp = PlPgSqlInterpreter::new().with_query_executor(|_| Ok(vec![]));

        let func = PlPgSqlFunction {
            declarations: vec![PlPgSqlStmt::Declare {
                name: "count".to_string(),
                data_type: "INT".to_string(),
                default: Some(Expr::Number(0)),
            }],
            body: vec![
                PlPgSqlStmt::ForQuery {
                    var: "rec".to_string(),
                    query: "SELECT * FROM empty".to_string(),
                    body: vec![PlPgSqlStmt::Assign {
                        target: "count".to_string(),
                        value: Expr::Number(1),
                    }],
                },
                PlPgSqlStmt::Return { value: Some(Expr::Column("count".to_string())) },
            ],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_split_part_out_of_bounds() {
        let result = BuiltinFunctions::execute(
            "split_part",
            vec![Value::Text("a,b".to_string()), Value::Text(",".to_string()), Value::Int(5)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_split_part_empty_string() {
        let result = BuiltinFunctions::execute(
            "split_part",
            vec![Value::Text("".to_string()), Value::Text(",".to_string()), Value::Int(1)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_not_operator_zero() {
        use crate::parser::ast::UnaryOperator;
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
    fn test_not_operator_nonzero() {
        use crate::parser::ast::UnaryOperator;
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::Number(5)),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_string_concat_empty() {
        use crate::parser::ast::BinaryOperator;
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::String("".to_string())),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(Expr::String("".to_string())),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_perform_no_executor() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Perform { query: "SELECT 1".to_string() }],
        };

        let result = interp.execute(&func, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_like_pattern_match() {
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::String("test123".to_string())),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::String("test%".to_string())),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_array_operations() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let len = BuiltinFunctions::execute("array_length", vec![arr.clone()]).unwrap();
        assert_eq!(len, Value::Int(2));

        let appended = BuiltinFunctions::execute("array_append", vec![arr, Value::Int(3)]).unwrap();
        assert_eq!(appended, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
    }

    #[test]
    fn test_bool_and() {
        use crate::parser::ast::BinaryOperator;
        let mut interp = PlPgSqlInterpreter::new();
        let func = PlPgSqlFunction {
            declarations: vec![],
            body: vec![PlPgSqlStmt::Return {
                value: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Number(1)),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::Number(0)),
                }),
            }],
        };

        let result = interp.execute(&func, vec![]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_datetime_functions_positive() {
        let now = BuiltinFunctions::execute("now", vec![]).unwrap();
        let date = BuiltinFunctions::execute("current_date", vec![]).unwrap();

        if let (Value::Int(n), Value::Int(d)) = (now, date) {
            assert!(n > 0);
            assert!(d > 0);
            assert!(n / 86400 >= d);
        } else {
            panic!("Expected Int values");
        }
    }
}
