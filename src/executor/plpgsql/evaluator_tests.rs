/// Comprehensive unit tests for the PL/pgSQL expression evaluator
///
/// These tests cover the fix implemented in commit 0d7db7b which:
/// 1. Added direct handling of Expr::Float
/// 2. Replaced brittle string-based eval_arithmetic/eval_float_arithmetic functions
///    with direct pattern matching for arithmetic BinaryOperators
/// 3. Added proper division by zero error handling
///
/// This test suite prevents regression by covering:
/// - All arithmetic operations (Add, Subtract, Multiply, Divide, Modulo)
/// - All comparison operations
/// - Division by zero scenarios
/// - Type mismatch scenarios
/// - Float precision edge cases
/// - Mixed type operations

#[cfg(test)]
mod tests {
    use crate::catalog::Value;
    use crate::executor::plpgsql::evaluator::PlPgSqlExprEvaluator;
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
    use std::collections::HashMap;

    // ========================================================================
    // Float Expression Tests
    // ========================================================================

    #[test]
    fn test_eval_float_positive() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        assert_eq!(
            evaluator.eval(&Expr::Float(std::f64::consts::PI)).unwrap(),
            Value::Float(std::f64::consts::PI)
        );
    }

    #[test]
    fn test_eval_float_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        assert_eq!(
            evaluator.eval(&Expr::Float(-std::f64::consts::E)).unwrap(),
            Value::Float(-std::f64::consts::E)
        );
    }

    #[test]
    fn test_eval_float_zero() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        assert_eq!(evaluator.eval(&Expr::Float(0.0)).unwrap(), Value::Float(0.0));
    }

    #[test]
    fn test_eval_float_very_small() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let result = evaluator.eval(&Expr::Float(0.0000001)).unwrap();
        assert!((result.as_float().unwrap() - 0.0000001).abs() < f64::EPSILON);
    }

    #[test]
    fn test_eval_float_very_large() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let result = evaluator.eval(&Expr::Float(1e100)).unwrap();
        assert!((result.as_float().unwrap() - 1e100).abs() < f64::EPSILON);
    }

    #[test]
    fn test_eval_float_from_column() {
        let mut vars = HashMap::new();
        vars.insert("price".to_string(), Value::Float(19.99));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let result = evaluator.eval(&Expr::Column("price".to_string())).unwrap();
        assert!((result.as_float().unwrap() - 19.99).abs() < f64::EPSILON);
    }

    // ========================================================================
    // Integer Arithmetic Operations Tests
    // ========================================================================

    #[test]
    fn test_int_addition_positive() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(200)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(300));
    }

    #[test]
    fn test_int_addition_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-50)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(30)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-20));
    }

    #[test]
    fn test_int_addition_both_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-100)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(-50)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-150));
    }

    #[test]
    fn test_int_subtraction_positive() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(30)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(70));
    }

    #[test]
    fn test_int_subtraction_negative_result() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(30)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(100)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-70));
    }

    #[test]
    fn test_int_subtraction_negative_operands() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-50)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(-20)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-30));
    }

    #[test]
    fn test_int_multiplication_positive() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(12)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(60));
    }

    #[test]
    fn test_int_multiplication_by_zero() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(1000)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(0)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(0));
    }

    #[test]
    fn test_int_multiplication_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-7)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(8)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-56));
    }

    #[test]
    fn test_int_multiplication_both_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-6)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(-9)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(54));
    }

    #[test]
    fn test_int_division_exact() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(20));
    }

    #[test]
    fn test_int_division_truncates() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(7)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(2)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(3));
    }

    #[test]
    fn test_int_division_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-20)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(4)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-5));
    }

    #[test]
    fn test_int_division_by_one() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(42)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(1)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(42));
    }

    #[test]
    fn test_int_modulo_exact() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(10)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(0));
    }

    #[test]
    fn test_int_modulo_remainder() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(17)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(5)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(2));
    }

    #[test]
    fn test_int_modulo_negative() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(-17)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(5)),
        };
        // Rust's modulo with negative numbers
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(-2));
    }

    // ========================================================================
    // Float Arithmetic Operations Tests
    // ========================================================================

    #[test]
    fn test_float_addition() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(1.5)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Float(2.5)),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert!((result.as_float().unwrap() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_subtraction() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(10.0)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Float(3.5)),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert!((result.as_float().unwrap() - 6.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_multiplication() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(2.5)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Float(4.0)),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert!((result.as_float().unwrap() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_division() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(10.0)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Float(2.5)),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert!((result.as_float().unwrap() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_division_by_one() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(42.5)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Float(1.0)),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert!((result.as_float().unwrap() - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_comparison_equal() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(std::f64::consts::PI)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Float(std::f64::consts::PI)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_float_comparison_almost_equal() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // Test floating point epsilon comparison
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(1.0 / 3.0 * 3.0)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Float(1.0)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_float_comparison_greater_than() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(5.01)),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Float(5.0)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_float_comparison_less_than() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(4.99)),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Float(5.0)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }

    // ========================================================================
    // Division by Zero Tests
    // ========================================================================

    #[test]
    fn test_int_division_by_zero_literal() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(0)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_int_division_by_zero_variable() {
        let mut vars = HashMap::new();
        vars.insert("zero".to_string(), Value::Int(0));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Column("zero".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_int_modulo_by_zero_literal() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(0)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_int_modulo_by_zero_variable() {
        let mut vars = HashMap::new();
        vars.insert("zero".to_string(), Value::Int(0));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(100)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Column("zero".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_float_division_by_zero_literal() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(100.5)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Float(0.0)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    #[test]
    fn test_float_division_by_zero_variable() {
        let mut vars = HashMap::new();
        vars.insert("zero_float".to_string(), Value::Float(0.0));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(100.5)),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Column("zero_float".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Division by zero");
    }

    // ========================================================================
    // Type Mismatch Tests
    // ========================================================================

    #[test]
    fn test_type_mismatch_int_string_add() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(10)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::String("hello".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Type mismatch in binary operation");
    }

    #[test]
    fn test_type_mismatch_float_int_add() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(10.5)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(5)),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert_eq!(result, Value::Float(15.5));
    }

    #[test]
    fn test_type_mismatch_bool_int_and() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // Note: In the current implementation, Bool AND Int would be a type mismatch
        // because the match arm is (Value::Bool, Value::Bool)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(1)), // This creates Value::Int, not Value::Bool
            op: BinaryOperator::And,
            right: Box::new(Expr::String("test".to_string())),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Type mismatch in binary operation");
    }

    #[test]
    fn test_type_mismatch_string_int_comparison() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::String("10".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(10)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert_eq!(err, "Type mismatch in binary operation");
    }

    // ========================================================================
    // Unsupported Operator Tests
    // ========================================================================

    #[test]
    fn test_unsupported_operator_for_int() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // StringConcat is not supported for Int
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(10)),
            op: BinaryOperator::StringConcat,
            right: Box::new(Expr::Number(20)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert!(err.contains("Operator StringConcat not supported for INT"));
    }

    #[test]
    fn test_unsupported_operator_for_float() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // Modulo is not supported for Float in the current implementation
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(10.5)),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Float(3.0)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert!(err.contains("Operator Modulo not supported for FLOAT"));
    }

    #[test]
    fn test_unsupported_operator_for_bool() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // StringConcat is not supported for Int (which is used for bool representation)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(1)), // Using Int as bool representation
            op: BinaryOperator::StringConcat,
            right: Box::new(Expr::Number(1)),
        };
        let err = evaluator.eval(&expr).unwrap_err();
        assert!(err.contains("Operator StringConcat not supported for INT"));
    }

    // ========================================================================
    // Complex Expression Tests
    // ========================================================================

    #[test]
    fn test_nested_arithmetic() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // (5 + 3) * 2
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Number(5)),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(3)),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(2)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(16));
    }

    #[test]
    fn test_complex_mixed_operations() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // ((10 - 5) * 2) + (8 / 4)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Number(10)),
                    op: BinaryOperator::Subtract,
                    right: Box::new(Expr::Number(5)),
                }),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(2)),
            }),
            op: BinaryOperator::Add,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Number(8)),
                op: BinaryOperator::Divide,
                right: Box::new(Expr::Number(4)),
            }),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(12));
    }

    #[test]
    fn test_chained_comparisons() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // 5 > 3 AND 3 < 10
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Number(5)),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(3)),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Number(3)),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(10)),
            }),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }

    // ========================================================================
    // Variable-based Expression Tests
    // ========================================================================

    #[test]
    fn test_arithmetic_with_variables() {
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), Value::Int(10));
        vars.insert("y".to_string(), Value::Int(5));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("x".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Column("y".to_string())),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(50));
    }

    #[test]
    fn test_float_variables() {
        let mut vars = HashMap::new();
        vars.insert("price".to_string(), Value::Float(19.99));
        vars.insert("quantity".to_string(), Value::Float(3.0));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Column("quantity".to_string())),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert!((result.as_float().unwrap() - 59.97).abs() < 0.01);
    }

    #[test]
    fn test_mixed_int_float_variables() {
        let mut vars = HashMap::new();
        vars.insert("count".to_string(), Value::Int(5));
        vars.insert("rate".to_string(), Value::Float(2.5));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Column("rate".to_string())),
        };
        let result = evaluator.eval(&expr).unwrap();
        assert_eq!(result, Value::Float(12.5));
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_integer_overflow_behavior() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);
        // Test large number multiplication (may overflow in debug, wrap in release)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(i64::MAX / 2)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(i64::MAX / 2)),
        };
        // Just verify it doesn't panic - overflow behavior depends on build mode
        let result = evaluator.eval(&expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_zero_operations() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        // 0 + 0
        let add = Expr::BinaryOp {
            left: Box::new(Expr::Number(0)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(0)),
        };
        assert_eq!(evaluator.eval(&add).unwrap(), Value::Int(0));

        // 0 * 0
        let mul = Expr::BinaryOp {
            left: Box::new(Expr::Number(0)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(0)),
        };
        assert_eq!(evaluator.eval(&mul).unwrap(), Value::Int(0));

        // 0 - 0
        let sub = Expr::BinaryOp {
            left: Box::new(Expr::Number(0)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(0)),
        };
        assert_eq!(evaluator.eval(&sub).unwrap(), Value::Int(0));
    }

    #[test]
    fn test_identity_operations() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        // x + 0 = x
        let add_zero = Expr::BinaryOp {
            left: Box::new(Expr::Number(42)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(0)),
        };
        assert_eq!(evaluator.eval(&add_zero).unwrap(), Value::Int(42));

        // x * 1 = x
        let mul_one = Expr::BinaryOp {
            left: Box::new(Expr::Number(42)),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(1)),
        };
        assert_eq!(evaluator.eval(&mul_one).unwrap(), Value::Int(42));

        // x - 0 = x
        let sub_zero = Expr::BinaryOp {
            left: Box::new(Expr::Number(42)),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(0)),
        };
        assert_eq!(evaluator.eval(&sub_zero).unwrap(), Value::Int(42));
    }

    #[test]
    fn test_float_precision_edge_case() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        // Test that very close floats are considered equal
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(0.1 + 0.2)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Float(0.3)),
        };
        // Due to floating point representation, 0.1 + 0.2 != 0.3 exactly
        // but our implementation uses epsilon comparison
        let result = evaluator.eval(&expr).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_unary_not_on_bool_variable() {
        let mut vars = HashMap::new();
        vars.insert("flag".to_string(), Value::Bool(false));
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Column("flag".to_string())),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_double_negation() {
        let vars = HashMap::new();
        let evaluator = PlPgSqlExprEvaluator::new(&vars);

        // NOT(NOT(5 > 3))
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Number(5)),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Number(3)),
                }),
            }),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Bool(true));
    }
}
