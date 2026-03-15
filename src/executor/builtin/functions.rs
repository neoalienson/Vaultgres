use crate::catalog::{Function, FunctionLanguage, FunctionRegistry, Parameter, Value};

pub struct BuiltinFunctions;

impl BuiltinFunctions {
    pub fn register_all(registry: &mut FunctionRegistry) {
        Self::register_string_functions(registry);
        Self::register_math_functions(registry);
        Self::register_datetime_functions(registry);
        Self::register_array_functions(registry);
        Self::register_json_functions(registry);
    }

    fn register_string_functions(registry: &mut FunctionRegistry) {
        registry
            .register(Function {
                name: "upper".to_string(),
                parameters: vec![Parameter {
                    name: "s".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                }],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:upper".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "lower".to_string(),
                parameters: vec![Parameter {
                    name: "s".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                }],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:lower".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "length".to_string(),
                parameters: vec![Parameter {
                    name: "s".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:length".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "substring".to_string(),
                parameters: vec![
                    Parameter {
                        name: "s".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "start".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "len".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:substring".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "concat".to_string(),
                parameters: vec![Parameter {
                    name: "args".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                }],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:concat".to_string(),
                is_variadic: true,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "trim".to_string(),
                parameters: vec![Parameter {
                    name: "s".to_string(),
                    data_type: "TEXT".to_string(),
                    default: None,
                }],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:trim".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "replace".to_string(),
                parameters: vec![
                    Parameter {
                        name: "s".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "from".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "to".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                ],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:replace".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "split_part".to_string(),
                parameters: vec![
                    Parameter {
                        name: "s".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "delim".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "n".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:split_part".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
    }

    fn register_math_functions(registry: &mut FunctionRegistry) {
        registry
            .register(Function {
                name: "abs".to_string(),
                parameters: vec![Parameter {
                    name: "n".to_string(),
                    data_type: "INT".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:abs".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "power".to_string(),
                parameters: vec![
                    Parameter {
                        name: "base".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "exp".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:power".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "sqrt".to_string(),
                parameters: vec![Parameter {
                    name: "n".to_string(),
                    data_type: "INT".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:sqrt".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "mod".to_string(),
                parameters: vec![
                    Parameter {
                        name: "a".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "b".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:mod".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "round".to_string(),
                parameters: vec![Parameter {
                    name: "n".to_string(),
                    data_type: "INT".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:round".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "ceil".to_string(),
                parameters: vec![Parameter {
                    name: "n".to_string(),
                    data_type: "INT".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:ceil".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "floor".to_string(),
                parameters: vec![Parameter {
                    name: "n".to_string(),
                    data_type: "INT".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:floor".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "random".to_string(),
                parameters: vec![],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:random".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
    }

    fn register_datetime_functions(registry: &mut FunctionRegistry) {
        registry
            .register(Function {
                name: "now".to_string(),
                parameters: vec![],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:now".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "current_date".to_string(),
                parameters: vec![],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:current_date".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "extract".to_string(),
                parameters: vec![
                    Parameter {
                        name: "field".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "timestamp".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:extract".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "date_trunc".to_string(),
                parameters: vec![
                    Parameter {
                        name: "field".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "timestamp".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:date_trunc".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
    }

    fn register_array_functions(registry: &mut FunctionRegistry) {
        registry
            .register(Function {
                name: "array_length".to_string(),
                parameters: vec![Parameter {
                    name: "arr".to_string(),
                    data_type: "ARRAY".to_string(),
                    default: None,
                }],
                return_type: "INT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:array_length".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "array_append".to_string(),
                parameters: vec![
                    Parameter {
                        name: "arr".to_string(),
                        data_type: "ARRAY".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "elem".to_string(),
                        data_type: "INT".to_string(),
                        default: None,
                    },
                ],
                return_type: "ARRAY".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:array_append".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
    }

    fn register_json_functions(registry: &mut FunctionRegistry) {
        registry
            .register(Function {
                name: "json_object".to_string(),
                parameters: vec![],
                return_type: "JSON".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:json_object".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "json_array".to_string(),
                parameters: vec![],
                return_type: "JSON".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:json_array".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
        registry
            .register(Function {
                name: "json_extract".to_string(),
                parameters: vec![
                    Parameter {
                        name: "json".to_string(),
                        data_type: "JSON".to_string(),
                        default: None,
                    },
                    Parameter {
                        name: "path".to_string(),
                        data_type: "TEXT".to_string(),
                        default: None,
                    },
                ],
                return_type: "TEXT".to_string(),
                language: FunctionLanguage::Sql,
                body: "BUILTIN:json_extract".to_string(),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: crate::catalog::FunctionVolatility::Immutable,
            })
            .unwrap();
    }

    pub fn execute(name: &str, args: Vec<Value>) -> Result<Value, String> {
        match name {
            "upper" => {
                if let Value::Text(s) = &args[0] {
                    Ok(Value::Text(s.to_uppercase()))
                } else {
                    Err("Expected TEXT".to_string())
                }
            }
            "lower" => {
                if let Value::Text(s) = &args[0] {
                    Ok(Value::Text(s.to_lowercase()))
                } else {
                    Err("Expected TEXT".to_string())
                }
            }
            "length" => {
                if let Value::Text(s) = &args[0] {
                    Ok(Value::Int(s.len() as i64))
                } else {
                    Err("Expected TEXT".to_string())
                }
            }
            "substring" => {
                if let (Value::Text(s), Value::Int(start), Value::Int(len)) =
                    (&args[0], &args[1], &args[2])
                {
                    let start = (*start - 1).max(0) as usize;
                    let len = (*len).max(0) as usize;
                    let result = s.chars().skip(start).take(len).collect();
                    Ok(Value::Text(result))
                } else {
                    Err("Expected TEXT, INT, INT".to_string())
                }
            }
            "concat" => {
                let mut result = String::new();
                for val in args {
                    match val {
                        Value::Text(s) => result.push_str(&s),
                        Value::Int(i) => result.push_str(&i.to_string()),
                        Value::Null => continue,
                        _ => return Err("CONCAT requires text or numeric values".to_string()),
                    }
                }
                Ok(Value::Text(result))
            }
            "trim" => {
                if let Value::Text(s) = &args[0] {
                    Ok(Value::Text(s.trim().to_string()))
                } else {
                    Err("Expected TEXT".to_string())
                }
            }
            "replace" => {
                if let (Value::Text(s), Value::Text(from), Value::Text(to)) =
                    (&args[0], &args[1], &args[2])
                {
                    Ok(Value::Text(s.replace(from, to)))
                } else {
                    Err("Expected TEXT, TEXT, TEXT".to_string())
                }
            }
            "split_part" => {
                if let (Value::Text(s), Value::Text(delim), Value::Int(n)) =
                    (&args[0], &args[1], &args[2])
                {
                    let parts: Vec<&str> = s.split(delim.as_str()).collect();
                    let idx = (*n - 1).max(0) as usize;
                    if idx < parts.len() {
                        Ok(Value::Text(parts[idx].to_string()))
                    } else {
                        Ok(Value::Text(String::new()))
                    }
                } else {
                    Err("Expected TEXT, TEXT, INT".to_string())
                }
            }
            "abs" => {
                if let Value::Int(n) = args[0] {
                    Ok(Value::Int(n.abs()))
                } else {
                    Err("Expected INT".to_string())
                }
            }
            "power" => {
                if let (Value::Int(base), Value::Int(exp)) = (&args[0], &args[1]) {
                    if *exp < 0 {
                        Err("Negative exponent not supported".to_string())
                    } else {
                        Ok(Value::Int(base.pow(*exp as u32)))
                    }
                } else {
                    Err("Expected INT arguments".to_string())
                }
            }
            "sqrt" => {
                if let Value::Int(n) = args[0] {
                    if n < 0 {
                        Err("Cannot take square root of negative number".to_string())
                    } else {
                        Ok(Value::Int((n as f64).sqrt() as i64))
                    }
                } else {
                    Err("Expected INT".to_string())
                }
            }
            "mod" => {
                if let (Value::Int(a), Value::Int(b)) = (&args[0], &args[1]) {
                    if *b == 0 {
                        Err("Division by zero".to_string())
                    } else {
                        Ok(Value::Int(a % b))
                    }
                } else {
                    Err("Expected INT arguments".to_string())
                }
            }
            "round" => {
                if let Value::Int(n) = args[0] {
                    Ok(Value::Int(n))
                } else {
                    Err("Expected INT".to_string())
                }
            }
            "ceil" => {
                if let Value::Int(n) = args[0] {
                    Ok(Value::Int(n))
                } else {
                    Err("Expected INT".to_string())
                }
            }
            "floor" => {
                if let Value::Int(n) = args[0] {
                    Ok(Value::Int(n))
                } else {
                    Err("Expected INT".to_string())
                }
            }
            "random" => {
                use std::collections::hash_map::RandomState;
                use std::hash::BuildHasher;
                let s = RandomState::new();

                Ok(Value::Int((s.hash_one(std::time::SystemTime::now()) % 1000) as i64))
            }
            "now" => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let duration = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| format!("Time error: {}", e))?;
                Ok(Value::Int(duration.as_secs() as i64))
            }
            "current_date" => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let duration = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| format!("Time error: {}", e))?;
                let days = duration.as_secs() / 86400;
                Ok(Value::Int(days as i64))
            }
            "array_length" => {
                if let Value::Array(arr) = &args[0] {
                    Ok(Value::Int(arr.len() as i64))
                } else {
                    Err("Expected ARRAY".to_string())
                }
            }
            "array_append" => {
                if let Value::Array(arr) = &args[0] {
                    let mut new_arr = arr.clone();
                    new_arr.push(args[1].clone());
                    Ok(Value::Array(new_arr))
                } else {
                    Err("Expected ARRAY".to_string())
                }
            }
            "json_object" => Ok(Value::Json("{}".to_string())),
            "json_array" => Ok(Value::Json("[]".to_string())),
            "json_extract" => {
                if let (Value::Json(json), Value::Text(path)) = (&args[0], &args[1]) {
                    if path == "$" {
                        Ok(Value::Text(json.clone()))
                    } else if let Some(key) = path.strip_prefix("$.") {
                        if json.contains(&format!("\"{}\":", key)) {
                            let start =
                                json.find(&format!("\"{}\":", key)).unwrap() + key.len() + 3;
                            let rest = &json[start..];
                            let end = rest.find(&[',', '}'][..]).unwrap_or(rest.len());
                            let value = rest[..end].trim().trim_matches('"');
                            Ok(Value::Text(value.to_string()))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Err("Invalid JSON path".to_string())
                    }
                } else {
                    Err("Expected JSON, TEXT".to_string())
                }
            }
            "extract" => {
                if let (Value::Text(field), Value::Int(ts)) = (&args[0], &args[1]) {
                    let secs = *ts as u64;
                    match field.to_lowercase().as_str() {
                        "epoch" => Ok(Value::Int(*ts)),
                        "year" => Ok(Value::Int(1970 + (secs / 31536000) as i64)),
                        "month" => Ok(Value::Int(((secs % 31536000) / 2592000) as i64 + 1)),
                        "day" => Ok(Value::Int(((secs % 2592000) / 86400) as i64 + 1)),
                        "hour" => Ok(Value::Int(((secs % 86400) / 3600) as i64)),
                        "minute" => Ok(Value::Int(((secs % 3600) / 60) as i64)),
                        "second" => Ok(Value::Int((secs % 60) as i64)),
                        _ => Err(format!("Unknown field: {}", field)),
                    }
                } else {
                    Err("Expected TEXT, INT".to_string())
                }
            }
            "date_trunc" => {
                if let (Value::Text(field), Value::Int(ts)) = (&args[0], &args[1]) {
                    let secs = *ts as u64;
                    let truncated = match field.to_lowercase().as_str() {
                        "year" => (secs / 31536000) * 31536000,
                        "month" => (secs / 2592000) * 2592000,
                        "day" => (secs / 86400) * 86400,
                        "hour" => (secs / 3600) * 3600,
                        "minute" => (secs / 60) * 60,
                        "second" => secs,
                        _ => return Err(format!("Unknown field: {}", field)),
                    };
                    Ok(Value::Int(truncated as i64))
                } else {
                    Err("Expected TEXT, INT".to_string())
                }
            }
            _ => Err(format!("Unknown builtin function: {}", name)),
        }
    }
}
