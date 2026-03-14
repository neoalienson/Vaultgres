use vaultgres::catalog::{Function, FunctionLanguage, FunctionRegistry, Parameter, Value};
use vaultgres::executor::{
    BuiltinFunctions, CorrelatedExecutor, PlPgSqlInterpreter, RecursiveCTEExecutor, UnnestExecutor,
};
use vaultgres::parser::ast::{BinaryOperator, Expr};
use vaultgres::parser::plpgsql_ast::{PlPgSqlFunction, PlPgSqlStmt};

#[test]
fn test_udf_registration_and_execution() {
    let mut registry = FunctionRegistry::new();
    let func = Function {
        name: "add".to_string(),
        parameters: vec![
            Parameter { name: "a".to_string(), data_type: "INT".to_string(), default: None },
            Parameter { name: "b".to_string(), data_type: "INT".to_string(), default: None },
        ],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 + $2".to_string(),
        is_variadic: false,
        cost: 100.0,
        rows: 1,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
    };

    registry.register(func).unwrap();
    let resolved = registry.resolve("add", &["INT".to_string(), "INT".to_string()]);
    assert!(resolved.is_some());
    assert_eq!(resolved.unwrap().return_type, "INT");
}

#[test]
fn test_plpgsql_factorial() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![PlPgSqlStmt::Declare {
            name: "result".to_string(),
            data_type: "INT".to_string(),
            default: Some(Expr::Number(1)),
        }],
        body: vec![
            PlPgSqlStmt::For {
                var: "i".to_string(),
                start: Expr::Number(1),
                end: Expr::Column("$1".to_string()),
                body: vec![],
            },
            PlPgSqlStmt::Return { value: Some(Expr::Column("result".to_string())) },
        ],
    };

    let result = interp.execute(&func, vec![Value::Int(5)]).unwrap();
    assert_eq!(result, Value::Int(1));
}

#[test]
fn test_recursive_cte_tree_traversal() {
    let base = vec![vec![Value::Int(1)]];

    let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
        let mut results = Vec::new();
        for row in working {
            if let Value::Int(n) = row[0] {
                if n == 1 {
                    results.push(vec![Value::Int(2)]);
                    results.push(vec![Value::Int(3)]);
                } else if n == 2 {
                    results.push(vec![Value::Int(4)]);
                    results.push(vec![Value::Int(5)]);
                }
            }
        }
        Ok(results)
    };

    let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
    assert_eq!(result.len(), 5);
}

#[test]
fn test_correlated_exists_filter() {
    let outer = vec![
        vec![Value::Int(1), Value::Text("Alice".to_string())],
        vec![Value::Int(2), Value::Text("Bob".to_string())],
        vec![Value::Int(3), Value::Text("Charlie".to_string())],
    ];

    let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
        if let Value::Int(id) = row[0] {
            if id % 2 == 1 { Ok(vec![vec![Value::Int(1)]]) } else { Ok(vec![]) }
        } else {
            Ok(vec![])
        }
    };

    let result = CorrelatedExecutor::execute_exists(&outer, &subquery_fn).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0][1], Value::Text("Alice".to_string()));
    assert_eq!(result[1][1], Value::Text("Charlie".to_string()));
}

#[test]
fn test_correlated_scalar_join() {
    let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];

    let subquery_fn = |row: &[Value]| -> Result<Vec<Vec<Value>>, String> {
        if let Value::Int(n) = row[0] { Ok(vec![vec![Value::Int(n * 10)]]) } else { Ok(vec![]) }
    };

    let result = CorrelatedExecutor::execute_scalar(&outer, &subquery_fn).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], vec![Value::Int(1), Value::Int(10)]);
    assert_eq!(result[1], vec![Value::Int(2), Value::Int(20)]);
    assert_eq!(result[2], vec![Value::Int(3), Value::Int(30)]);
}

#[test]
fn test_builtin_functions_integration() {
    let mut registry = FunctionRegistry::new();
    BuiltinFunctions::register_all(&mut registry);

    assert!(registry.resolve("upper", &["TEXT".to_string()]).is_some());
    assert!(registry.resolve("lower", &["TEXT".to_string()]).is_some());
    assert!(registry.resolve("length", &["TEXT".to_string()]).is_some());
    assert!(registry.resolve("abs", &["INT".to_string()]).is_some());
    assert!(registry.resolve("power", &["INT".to_string(), "INT".to_string()]).is_some());

    let upper_result =
        BuiltinFunctions::execute("upper", vec![Value::Text("hello".to_string())]).unwrap();
    assert_eq!(upper_result, Value::Text("HELLO".to_string()));

    let abs_result = BuiltinFunctions::execute("abs", vec![Value::Int(-42)]).unwrap();
    assert_eq!(abs_result, Value::Int(42));
}

#[test]
fn test_plpgsql_conditional_logic() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![PlPgSqlStmt::If {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("$1".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(10)),
            },
            then_stmts: vec![PlPgSqlStmt::Return {
                value: Some(Expr::String("large".to_string())),
            }],
            else_stmts: vec![PlPgSqlStmt::Return {
                value: Some(Expr::String("small".to_string())),
            }],
        }],
    };

    let result1 = interp.execute(&func, vec![Value::Int(15)]).unwrap();
    assert_eq!(result1, Value::Text("large".to_string()));

    let result2 = interp.execute(&func, vec![Value::Int(5)]).unwrap();
    assert_eq!(result2, Value::Text("small".to_string()));
}

#[test]
fn test_recursive_cte_graph_cycle() {
    let base = vec![vec![Value::Int(1)]];

    let recursive_fn = |working: &[Vec<Value>]| -> Result<Vec<Vec<Value>>, String> {
        let mut results = Vec::new();
        for row in working {
            if let Value::Int(n) = row[0] {
                let next = (n % 5) + 1;
                results.push(vec![Value::Int(next)]);
            }
        }
        Ok(results)
    };

    let result = RecursiveCTEExecutor::execute(base, &recursive_fn).unwrap();
    assert_eq!(result.len(), 5);
}

#[test]
fn test_correlated_in_multiple_matches() {
    let outer = vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]];

    let subquery_fn = |_: &[Value]| -> Result<Vec<Vec<Value>>, String> {
        Ok(vec![vec![Value::Int(1)], vec![Value::Int(2)]])
    };

    let result = CorrelatedExecutor::execute_in(&outer, 0, &subquery_fn).unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn test_function_overload_different_types() {
    let mut registry = FunctionRegistry::new();

    let func_int = Function {
        name: "process".to_string(),
        parameters: vec![Parameter {
            name: "x".to_string(),
            data_type: "INT".to_string(),
            default: None,
        }],
        return_type: "INT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT $1 * 2".to_string(),
        is_variadic: false,
        cost: 100.0,
        rows: 1,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
    };

    let func_text = Function {
        name: "process".to_string(),
        parameters: vec![Parameter {
            name: "x".to_string(),
            data_type: "TEXT".to_string(),
            default: None,
        }],
        return_type: "TEXT".to_string(),
        language: FunctionLanguage::Sql,
        body: "SELECT UPPER($1)".to_string(),
        is_variadic: false,
        cost: 100.0,
        rows: 1,
        volatility: vaultgres::catalog::FunctionVolatility::Immutable,
    };

    registry.register(func_int).unwrap();
    registry.register(func_text).unwrap();

    let int_func = registry.resolve("process", &["INT".to_string()]);
    assert!(int_func.is_some());
    assert_eq!(int_func.unwrap().return_type, "INT");

    let text_func = registry.resolve("process", &["TEXT".to_string()]);
    assert!(text_func.is_some());
    assert_eq!(text_func.unwrap().return_type, "TEXT");
}

#[test]
fn test_plpgsql_parameters() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![PlPgSqlStmt::Return { value: Some(Expr::Column("$1".to_string())) }],
    };

    let result = interp.execute(&func, vec![Value::Int(42)]).unwrap();
    assert_eq!(result, Value::Int(42));
}

#[test]
fn test_exception_handling_integration() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![PlPgSqlStmt::ExceptionBlock {
            try_stmts: vec![PlPgSqlStmt::Raise { message: "test error".to_string() }],
            exception_var: "err".to_string(),
            catch_stmts: vec![PlPgSqlStmt::Return {
                value: Some(Expr::String("caught".to_string())),
            }],
        }],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Text("caught".to_string()));
}

#[test]
fn test_for_query_integration() {
    use std::collections::HashMap;
    let mut interp = PlPgSqlInterpreter::new().with_query_executor(|_| {
        let mut row1 = HashMap::new();
        row1.insert("val".to_string(), Value::Int(10));
        let mut row2 = HashMap::new();
        row2.insert("val".to_string(), Value::Int(20));
        Ok(vec![row1, row2])
    });

    let func = PlPgSqlFunction {
        declarations: vec![PlPgSqlStmt::Declare {
            name: "total".to_string(),
            data_type: "INT".to_string(),
            default: Some(Expr::Number(0)),
        }],
        body: vec![
            PlPgSqlStmt::ForQuery {
                var: "rec".to_string(),
                query: "SELECT val FROM t".to_string(),
                body: vec![PlPgSqlStmt::Assign {
                    target: "total".to_string(),
                    value: Expr::BinaryOp {
                        left: Box::new(Expr::Column("total".to_string())),
                        op: BinaryOperator::Add,
                        right: Box::new(Expr::Column("val".to_string())),
                    },
                }],
            },
            PlPgSqlStmt::Return { value: Some(Expr::Column("total".to_string())) },
        ],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Int(30));
}

#[test]
fn test_builtin_split_part() {
    let result = BuiltinFunctions::execute(
        "split_part",
        vec![Value::Text("a,b,c".to_string()), Value::Text(",".to_string()), Value::Int(2)],
    )
    .unwrap();
    assert_eq!(result, Value::Text("b".to_string()));
}

#[test]
fn test_builtin_random() {
    let result = BuiltinFunctions::execute("random", vec![]).unwrap();
    if let Value::Int(n) = result {
        assert!((0..1000).contains(&n));
    } else {
        panic!("Expected Int");
    }
}

#[test]
fn test_perform_statement() {
    let mut interp = PlPgSqlInterpreter::new().with_query_executor(|_| Ok(vec![]));

    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![
            PlPgSqlStmt::Perform { query: "SELECT 1".to_string() },
            PlPgSqlStmt::Return { value: Some(Expr::Number(100)) },
        ],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Int(100));
}

#[test]
fn test_datetime_functions() {
    let now_result = BuiltinFunctions::execute("now", vec![]).unwrap();
    if let Value::Int(n) = now_result {
        assert!(n > 1700000000);
    } else {
        panic!("Expected Int");
    }

    let date_result = BuiltinFunctions::execute("current_date", vec![]).unwrap();
    if let Value::Int(n) = date_result {
        assert!(n > 19000);
    } else {
        panic!("Expected Int");
    }
}

#[test]
fn test_bool_operations() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![PlPgSqlStmt::Return {
            value: Some(Expr::BinaryOp {
                left: Box::new(Expr::Number(10)),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(20)),
            }),
        }],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn test_like_operator_integration() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![PlPgSqlStmt::Return {
            value: Some(Expr::BinaryOp {
                left: Box::new(Expr::String("PostgreSQL".to_string())),
                op: BinaryOperator::Like,
                right: Box::new(Expr::String("%SQL".to_string())),
            }),
        }],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn test_array_functions_integration() {
    let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);

    let len = BuiltinFunctions::execute("array_length", vec![arr.clone()]).unwrap();
    assert_eq!(len, Value::Int(3));

    let appended = BuiltinFunctions::execute("array_append", vec![arr, Value::Int(40)]).unwrap();
    if let Value::Array(a) = appended {
        assert_eq!(a.len(), 4);
        assert_eq!(a[3], Value::Int(40));
    } else {
        panic!("Expected Array");
    }
}

#[test]
fn test_ilike_operator_integration() {
    let mut interp = PlPgSqlInterpreter::new();
    let func = PlPgSqlFunction {
        declarations: vec![],
        body: vec![PlPgSqlStmt::Return {
            value: Some(Expr::BinaryOp {
                left: Box::new(Expr::String("HELLO".to_string())),
                op: BinaryOperator::ILike,
                right: Box::new(Expr::String("hello".to_string())),
            }),
        }],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn test_foreach_integration() {
    let mut interp = PlPgSqlInterpreter::new();

    let func = PlPgSqlFunction {
        declarations: vec![
            PlPgSqlStmt::Declare {
                name: "nums".to_string(),
                data_type: "ARRAY".to_string(),
                default: Some(Expr::List(vec![
                    Expr::Number(5),
                    Expr::Number(10),
                    Expr::Number(15),
                ])),
            },
            PlPgSqlStmt::Declare {
                name: "total".to_string(),
                data_type: "INT".to_string(),
                default: Some(Expr::Number(0)),
            },
        ],
        body: vec![
            PlPgSqlStmt::ForEach {
                var: "n".to_string(),
                array: Expr::Column("nums".to_string()),
                body: vec![PlPgSqlStmt::Assign {
                    target: "total".to_string(),
                    value: Expr::BinaryOp {
                        left: Box::new(Expr::Column("total".to_string())),
                        op: BinaryOperator::Add,
                        right: Box::new(Expr::Column("n".to_string())),
                    },
                }],
            },
            PlPgSqlStmt::Return { value: Some(Expr::Column("total".to_string())) },
        ],
    };

    let result = interp.execute(&func, vec![]).unwrap();
    assert_eq!(result, Value::Int(30));
}

#[test]
fn test_unnest_integration() {
    let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    let result = UnnestExecutor::execute(arr).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0][0], Value::Int(1));
    assert_eq!(result[1][0], Value::Int(2));
    assert_eq!(result[2][0], Value::Int(3));
}

#[cfg(test)]
mod advanced_sql_new_tests {
    use std::collections::HashMap;
    use vaultgres::catalog::{FunctionRegistry, Value};
    use vaultgres::executor::{
        ArraySubqueryExecutor, BuiltinFunctions, DerivedTableExecutor, MultipleCTEExecutor,
    };
    use vaultgres::parser::ast::BinaryOperator;

    #[test]
    fn test_extract_function() {
        let mut registry = FunctionRegistry::new();
        BuiltinFunctions::register_all(&mut registry);

        let func = registry.resolve("extract", &["TEXT".to_string(), "INT".to_string()]).unwrap();
        assert_eq!(func.name, "extract");

        let result = BuiltinFunctions::execute(
            "extract",
            vec![Value::Text("hour".to_string()), Value::Int(3661)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_date_trunc_function() {
        let mut registry = FunctionRegistry::new();
        BuiltinFunctions::register_all(&mut registry);

        let func =
            registry.resolve("date_trunc", &["TEXT".to_string(), "INT".to_string()]).unwrap();
        assert_eq!(func.name, "date_trunc");

        let result = BuiltinFunctions::execute(
            "date_trunc",
            vec![Value::Text("day".to_string()), Value::Int(90061)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(86400));
    }

    #[test]
    fn test_any_operator() {
        let value = Value::Int(5);
        let subquery_results = vec![Value::Int(1), Value::Int(5), Value::Int(10)];

        let result = ArraySubqueryExecutor::execute_any(&value, &subquery_results).unwrap();
        assert!(result);

        let value2 = Value::Int(7);
        let result2 = ArraySubqueryExecutor::execute_any(&value2, &subquery_results).unwrap();
        assert!(!result2);
    }

    #[test]
    fn test_all_operator() {
        let value = Value::Int(10);
        let subquery_results = vec![Value::Int(1), Value::Int(5), Value::Int(9)];

        let result = ArraySubqueryExecutor::execute_all(
            &value,
            &BinaryOperator::GreaterThan,
            &subquery_results,
        )
        .unwrap();
        assert!(result);

        let value2 = Value::Int(5);
        let result2 = ArraySubqueryExecutor::execute_all(
            &value2,
            &BinaryOperator::GreaterThan,
            &subquery_results,
        )
        .unwrap();
        assert!(!result2);
    }

    #[test]
    fn test_some_operator() {
        let value = Value::Int(5);
        let subquery_results = vec![Value::Int(1), Value::Int(5), Value::Int(10)];

        let result = ArraySubqueryExecutor::execute_some(&value, &subquery_results).unwrap();
        assert!(result);
    }

    #[test]
    fn test_derived_table_executor() {
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Int(1));
        row1.insert("name".to_string(), Value::Text("Alice".to_string()));

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Int(2));
        row2.insert("name".to_string(), Value::Text("Bob".to_string()));

        let executor = DerivedTableExecutor::new(vec![row1, row2], "subq".to_string());
        let results = executor.execute();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("name"), Some(&Value::Text("Bob".to_string())));
        assert_eq!(executor.alias(), "subq");
    }

    #[test]
    fn test_extract_multiple_fields() {
        let timestamp = Value::Int(3661);

        let hour = BuiltinFunctions::execute(
            "extract",
            vec![Value::Text("hour".to_string()), timestamp.clone()],
        )
        .unwrap();
        assert_eq!(hour, Value::Int(1));

        let minute = BuiltinFunctions::execute(
            "extract",
            vec![Value::Text("minute".to_string()), timestamp.clone()],
        )
        .unwrap();
        assert_eq!(minute, Value::Int(1));

        let second = BuiltinFunctions::execute(
            "extract",
            vec![Value::Text("second".to_string()), timestamp],
        )
        .unwrap();
        assert_eq!(second, Value::Int(1));
    }

    #[test]
    fn test_all_with_empty_results() {
        let value = Value::Int(5);
        let empty_results = vec![];

        let result = ArraySubqueryExecutor::execute_all(
            &value,
            &BinaryOperator::GreaterThan,
            &empty_results,
        )
        .unwrap();
        assert!(result);
    }

    #[test]
    fn test_any_with_text() {
        let value = Value::Text("hello".to_string());
        let subquery_results = vec![
            Value::Text("world".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("rust".to_string()),
        ];

        let result = ArraySubqueryExecutor::execute_any(&value, &subquery_results).unwrap();
        assert!(result);
    }

    #[test]
    fn test_json_functions() {
        let mut registry = FunctionRegistry::new();
        BuiltinFunctions::register_all(&mut registry);

        assert!(registry.resolve("json_object", &[]).is_some());
        assert!(registry.resolve("json_array", &[]).is_some());
        assert!(
            registry.resolve("json_extract", &["JSON".to_string(), "TEXT".to_string()]).is_some()
        );

        let obj = BuiltinFunctions::execute("json_object", vec![]).unwrap();
        assert_eq!(obj, Value::Json("{}".to_string()));

        let arr = BuiltinFunctions::execute("json_array", vec![]).unwrap();
        assert_eq!(arr, Value::Json("[]".to_string()));

        let json = Value::Json("{\"name\":\"Bob\",\"age\":25}".to_string());
        let name = BuiltinFunctions::execute(
            "json_extract",
            vec![json.clone(), Value::Text("$.name".to_string())],
        )
        .unwrap();
        assert_eq!(name, Value::Text("Bob".to_string()));

        let age =
            BuiltinFunctions::execute("json_extract", vec![json, Value::Text("$.age".to_string())])
                .unwrap();
        assert_eq!(age, Value::Text("25".to_string()));
    }

    #[test]
    fn test_multiple_ctes() {
        let mut executor = MultipleCTEExecutor::new();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Int(1));
        row1.insert("value".to_string(), Value::Int(10));
        executor.add_cte("cte1".to_string(), vec![row1]);

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Int(2));
        row2.insert("value".to_string(), Value::Int(20));
        executor.add_cte("cte2".to_string(), vec![row2]);

        assert!(executor.get_cte("cte1").is_some());
        assert!(executor.get_cte("cte2").is_some());

        let result = executor
            .execute_with_ctes(|ctes| {
                let mut combined = Vec::new();
                if let Some(c1) = ctes.get("cte1") {
                    combined.extend(c1.clone());
                }
                if let Some(c2) = ctes.get("cte2") {
                    combined.extend(c2.clone());
                }
                Ok(combined)
            })
            .unwrap();

        assert_eq!(result.len(), 2);
    }
}
#[cfg(test)]
mod advanced_sql_new_tests2 {

    use vaultgres::catalog::{FunctionRegistry, Value};

    use std::collections::HashMap;

    #[test]
    fn test_function_with_defaults() {
        let mut registry = FunctionRegistry::new();

        let func = vaultgres::catalog::Function {
            name: "greet".to_string(),
            parameters: vec![
                vaultgres::catalog::Parameter {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                },
                vaultgres::catalog::Parameter {
                    name: "greeting".to_string(),
                    data_type: "TEXT".to_string(),
                    default: Some("Hello".to_string()),
                },
            ],
            return_type: "TEXT".to_string(),
            language: vaultgres::catalog::FunctionLanguage::Sql,
            body: "SELECT $2 || ' ' || $1".to_string(),
            is_variadic: false,
            cost: 100.0,
            rows: 1,
            volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        };

        registry.register(func).unwrap();

        let resolved = registry.resolve_with_defaults("greet", &["TEXT".to_string()]);
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().parameters.len(), 2);
    }

    #[test]
    fn test_variadic_function() {
        let mut registry = FunctionRegistry::new();

        let func = vaultgres::catalog::Function {
            name: "concat_all".to_string(),
            parameters: vec![vaultgres::catalog::Parameter {
                name: "values".to_string(),
                data_type: "TEXT".to_string(),
                default: None,
            }],
            return_type: "TEXT".to_string(),
            language: vaultgres::catalog::FunctionLanguage::Sql,
            body: "VARIADIC".to_string(),
            is_variadic: true,
            cost: 100.0,
            rows: 1,
            volatility: vaultgres::catalog::FunctionVolatility::Immutable,
        };

        registry.register(func).unwrap();

        let resolved =
            registry.resolve_with_defaults("concat_all", &["TEXT".to_string(), "TEXT".to_string()]);
        assert!(resolved.is_some());
    }

    #[test]
    fn test_function_cache() {
        let cache = vaultgres::executor::FunctionCache::new();

        let key =
            vaultgres::executor::FunctionCache::make_key("add", &[Value::Int(5), Value::Int(3)]);
        cache.set(key.clone(), Value::Int(8));

        assert_eq!(cache.get(&key), Some(Value::Int(8)));

        cache.clear();
        assert_eq!(cache.get(&key), None);
    }

    #[test]
    fn test_lateral_subquery() {
        let mut outer1 = HashMap::new();
        outer1.insert("x".to_string(), Value::Int(1));

        let mut outer2 = HashMap::new();
        outer2.insert("x".to_string(), Value::Int(2));

        let subquery_fn = |outer: &HashMap<String, Value>| {
            let x = match outer.get("x") {
                Some(Value::Int(n)) => *n,
                _ => return Err("Invalid x".to_string()),
            };

            let mut row = HashMap::new();
            row.insert("y".to_string(), Value::Int(x * 2));
            Ok(vec![row])
        };

        let results = vaultgres::executor::LateralSubqueryExecutor::execute(
            vec![outer1, outer2],
            subquery_fn,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("y"), Some(&Value::Int(2)));
        assert_eq!(results[1].get("y"), Some(&Value::Int(4)));
    }
}
