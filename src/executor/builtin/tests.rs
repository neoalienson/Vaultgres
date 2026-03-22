#[cfg(test)]
mod tests {
    use crate::catalog::{FunctionRegistry, Value};
    use crate::executor::builtin::BuiltinFunctions;

    #[test]
    fn test_upper() {
        let result =
            BuiltinFunctions::execute("upper", vec![Value::Text("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Text("HELLO".to_string()));
    }

    #[test]
    fn test_lower() {
        let result =
            BuiltinFunctions::execute("lower", vec![Value::Text("WORLD".to_string())]).unwrap();
        assert_eq!(result, Value::Text("world".to_string()));
    }

    #[test]
    fn test_length() {
        let result =
            BuiltinFunctions::execute("length", vec![Value::Text("test".to_string())]).unwrap();
        assert_eq!(result, Value::Int(4));
    }

    #[test]
    fn test_abs_positive() {
        let result = BuiltinFunctions::execute("abs", vec![Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_abs_negative() {
        let result = BuiltinFunctions::execute("abs", vec![Value::Int(-42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_power() {
        let result =
            BuiltinFunctions::execute("power", vec![Value::Int(2), Value::Int(3)]).unwrap();
        assert_eq!(result, Value::Int(8));
    }

    #[test]
    fn test_power_zero() {
        let result =
            BuiltinFunctions::execute("power", vec![Value::Int(5), Value::Int(0)]).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_register_all() {
        let mut registry = FunctionRegistry::new();
        BuiltinFunctions::register_all(&mut registry);

        assert!(registry.resolve("upper", &["TEXT".to_string()]).is_some());
        assert!(registry.resolve("lower", &["TEXT".to_string()]).is_some());
        assert!(registry.resolve("length", &["TEXT".to_string()]).is_some());
        assert!(registry
            .resolve("substring", &["TEXT".to_string(), "INT".to_string(), "INT".to_string()])
            .is_some());
        assert!(registry.resolve("concat", &["TEXT".to_string(), "TEXT".to_string()]).is_some());
        assert!(registry.resolve("trim", &["TEXT".to_string()]).is_some());
        assert!(registry
            .resolve("replace", &["TEXT".to_string(), "TEXT".to_string(), "TEXT".to_string()])
            .is_some());
        assert!(registry.resolve("abs", &["INT".to_string()]).is_some());
        assert!(registry.resolve("power", &["INT".to_string(), "INT".to_string()]).is_some());
        assert!(registry.resolve("sqrt", &["INT".to_string()]).is_some());
        assert!(registry.resolve("mod", &["INT".to_string(), "INT".to_string()]).is_some());
        assert!(registry.resolve("round", &["INT".to_string()]).is_some());
        assert!(registry.resolve("ceil", &["INT".to_string()]).is_some());
        assert!(registry.resolve("floor", &["INT".to_string()]).is_some());
        assert!(registry.resolve("random", &[]).is_some());
        assert!(registry
            .resolve("split_part", &["TEXT".to_string(), "TEXT".to_string(), "INT".to_string()])
            .is_some());
        assert!(registry.resolve("now", &[]).is_some());
        assert!(registry.resolve("current_date", &[]).is_some());
        assert!(registry.resolve("array_length", &["ARRAY".to_string()]).is_some());
        assert!(registry
            .resolve("array_append", &["ARRAY".to_string(), "INT".to_string()])
            .is_some());
    }

    #[test]
    fn test_substring() {
        let result = BuiltinFunctions::execute(
            "substring",
            vec![Value::Text("hello world".to_string()), Value::Int(1), Value::Int(5)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_concat() {
        let result = BuiltinFunctions::execute(
            "concat",
            vec![Value::Text("hello".to_string()), Value::Text(" world".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello world".to_string()));
    }

    #[test]
    fn test_concat_variadic_three_args() {
        let result = BuiltinFunctions::execute(
            "concat",
            vec![
                Value::Text("hello".to_string()),
                Value::Text(" ".to_string()),
                Value::Text("world".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello world".to_string()));
    }

    #[test]
    fn test_concat_variadic_many_args() {
        let result = BuiltinFunctions::execute(
            "concat",
            vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
                Value::Text("c".to_string()),
                Value::Text("d".to_string()),
                Value::Text("e".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::Text("abcde".to_string()));
    }

    #[test]
    fn test_concat_with_int() {
        let result = BuiltinFunctions::execute(
            "concat",
            vec![Value::Text("Value: ".to_string()), Value::Int(42)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("Value: 42".to_string()));
    }

    #[test]
    fn test_concat_mixed_types() {
        let result = BuiltinFunctions::execute(
            "concat",
            vec![
                Value::Text("SKU".to_string()),
                Value::Text(" - ".to_string()),
                Value::Int(123),
                Value::Text(" - ".to_string()),
                Value::Text("Product".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::Text("SKU - 123 - Product".to_string()));
    }

    #[test]
    fn test_concat_with_null() {
        let result = BuiltinFunctions::execute(
            "concat",
            vec![Value::Text("hello".to_string()), Value::Null, Value::Text("world".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_concat_all_nulls() {
        let result =
            BuiltinFunctions::execute("concat", vec![Value::Null, Value::Null, Value::Null])
                .unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_concat_empty_args() {
        let result = BuiltinFunctions::execute("concat", vec![]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_concat_single_arg() {
        let result =
            BuiltinFunctions::execute("concat", vec![Value::Text("single".to_string())]).unwrap();
        assert_eq!(result, Value::Text("single".to_string()));
    }

    #[test]
    fn test_concat_invalid_type() {
        let result = BuiltinFunctions::execute("concat", vec![Value::Bool(true)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CONCAT requires text or numeric values"));
    }

    #[test]
    fn test_concat_variadic_in_registry() {
        let mut registry = FunctionRegistry::new();
        BuiltinFunctions::register_all(&mut registry);

        // Test that concat resolves with 2 args
        assert!(registry
            .resolve_with_defaults("concat", &["TEXT".to_string(), "TEXT".to_string()])
            .is_some());

        // Test that concat resolves with 3 args
        assert!(registry
            .resolve_with_defaults(
                "concat",
                &["TEXT".to_string(), "TEXT".to_string(), "TEXT".to_string()]
            )
            .is_some());

        // Test that concat resolves with 5 args
        assert!(registry
            .resolve_with_defaults(
                "concat",
                &[
                    "TEXT".to_string(),
                    "TEXT".to_string(),
                    "TEXT".to_string(),
                    "TEXT".to_string(),
                    "TEXT".to_string()
                ]
            )
            .is_some());

        // Test that concat resolves with mixed types (TEXT and INT)
        assert!(registry
            .resolve_with_defaults("concat", &["TEXT".to_string(), "INT".to_string()])
            .is_some());
    }

    #[test]
    fn test_trim() {
        let result =
            BuiltinFunctions::execute("trim", vec![Value::Text("  hello  ".to_string())]).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_sqrt() {
        let result = BuiltinFunctions::execute("sqrt", vec![Value::Int(16)]).unwrap();
        assert_eq!(result, Value::Int(4));
    }

    #[test]
    fn test_mod() {
        let result = BuiltinFunctions::execute("mod", vec![Value::Int(10), Value::Int(3)]).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_replace() {
        let result = BuiltinFunctions::execute(
            "replace",
            vec![
                Value::Text("hello world".to_string()),
                Value::Text("world".to_string()),
                Value::Text("rust".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::Text("hello rust".to_string()));
    }

    #[test]
    fn test_round() {
        let result = BuiltinFunctions::execute("round", vec![Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_ceil() {
        let result = BuiltinFunctions::execute("ceil", vec![Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_floor() {
        let result = BuiltinFunctions::execute("floor", vec![Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_now() {
        let result = BuiltinFunctions::execute("now", vec![]).unwrap();
        if let Value::Int(n) = result {
            assert!(n > 0);
        } else {
            panic!("Expected Int");
        }
    }

    #[test]
    fn test_current_date() {
        let result = BuiltinFunctions::execute("current_date", vec![]).unwrap();
        if let Value::Int(n) = result {
            assert!(n > 0);
        } else {
            panic!("Expected Int");
        }
    }

    #[test]
    fn test_array_length() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = BuiltinFunctions::execute("array_length", vec![arr]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_array_append() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = BuiltinFunctions::execute("array_append", vec![arr, Value::Int(3)]).unwrap();
        assert_eq!(result, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
    }

    #[test]
    fn test_split_part() {
        let result = BuiltinFunctions::execute(
            "split_part",
            vec![Value::Text("a,b,c".to_string()), Value::Text(",".to_string()), Value::Int(2)],
        )
        .unwrap();
        assert_eq!(result, Value::Text("b".to_string()));
    }

    #[test]
    fn test_random() {
        let result = BuiltinFunctions::execute("random", vec![]).unwrap();
        if let Value::Int(n) = result {
            assert!((0..1000).contains(&n));
        } else {
            panic!("Expected Int");
        }
    }

    #[test]
    fn test_extract_year() {
        let result = BuiltinFunctions::execute(
            "extract",
            vec![Value::Text("year".to_string()), Value::Int(1609459200)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(2021));
    }

    #[test]
    fn test_extract_hour() {
        let result = BuiltinFunctions::execute(
            "extract",
            vec![Value::Text("hour".to_string()), Value::Int(3661)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_date_trunc_day() {
        let result = BuiltinFunctions::execute(
            "date_trunc",
            vec![Value::Text("day".to_string()), Value::Int(90061)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(86400));
    }

    #[test]
    fn test_date_trunc_hour() {
        let result = BuiltinFunctions::execute(
            "date_trunc",
            vec![Value::Text("hour".to_string()), Value::Int(7261)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(7200));
    }

    #[test]
    fn test_json_object() {
        let result = BuiltinFunctions::execute("json_object", vec![]).unwrap();
        assert_eq!(result, Value::Json("{}".to_string()));
    }

    #[test]
    fn test_json_array() {
        let result = BuiltinFunctions::execute("json_array", vec![]).unwrap();
        assert_eq!(result, Value::Json("[]".to_string()));
    }

    #[test]
    fn test_json_extract() {
        let json = Value::Json("{\"name\":\"Alice\",\"age\":30}".to_string());
        let result = BuiltinFunctions::execute(
            "json_extract",
            vec![json, Value::Text("$.name".to_string())],
        )
        .unwrap();
        assert_eq!(result, Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_json_extract_missing_key() {
        let json = Value::Json("{\"name\":\"Alice\"}".to_string());
        let result =
            BuiltinFunctions::execute("json_extract", vec![json, Value::Text("$.age".to_string())])
                .unwrap();
        assert_eq!(result, Value::Null);
    }
}
