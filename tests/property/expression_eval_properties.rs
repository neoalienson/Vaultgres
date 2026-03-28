use std::collections::HashMap;
use vaultgres::executor::Eval;
use vaultgres::parser::ast::{BinaryOperator, Expr, UnaryOperator};

#[test]
fn test_binary_add_ints() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(5)),
        op: BinaryOperator::Add,
        right: Box::new(Expr::Number(3)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_binary_subtract_ints() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(10)),
        op: BinaryOperator::Subtract,
        right: Box::new(Expr::Number(3)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_binary_multiply_ints() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(4)),
        op: BinaryOperator::Multiply,
        right: Box::new(Expr::Number(7)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_binary_divide_ints() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(20)),
        op: BinaryOperator::Divide,
        right: Box::new(Expr::Number(4)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_binary_equals_ints() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(42)),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(42)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_binary_not_equals_ints() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(10)),
        op: BinaryOperator::NotEquals,
        right: Box::new(Expr::Number(20)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_unary_minus_int() {
    let expr = Expr::UnaryOp { op: UnaryOperator::Minus, expr: Box::new(Expr::Number(42)) };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
    if let Ok(val) = result {
        assert_eq!(val, vaultgres::catalog::Value::Int(-42));
    }
}

#[test]
fn test_unary_not_comparison() {
    let expr = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        }),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_expr_clone_preserves_value() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(5)),
        op: BinaryOperator::Add,
        right: Box::new(Expr::Number(3)),
    };
    let cloned = expr.clone();
    assert_eq!(expr, cloned);
}

#[test]
fn test_nested_binary_operations() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(2)),
        }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expr::Number(3)),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_string_concat() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::String("Hello".to_string())),
        op: BinaryOperator::StringConcat,
        right: Box::new(Expr::String(" World".to_string())),
    };
    let result = Eval::eval_expr(&expr, &HashMap::new());
    assert!(result.is_ok());
}

#[test]
fn test_column_expression_lookup() {
    let expr = Expr::Column("id".to_string());
    let mut tuple = HashMap::new();
    tuple.insert("id".to_string(), vaultgres::catalog::Value::Int(42));

    let result = Eval::eval_expr(&expr, &tuple);
    assert!(result.is_ok());
}

#[test]
fn test_alias_expr_roundtrip() {
    let expr = Expr::Alias { expr: Box::new(Expr::Number(42)), alias: "the_answer".to_string() };
    let cloned = expr.clone();
    if let Expr::Alias { expr: _, alias } = cloned {
        assert_eq!(alias, "the_answer");
    } else {
        panic!("Clone did not preserve Alias variant");
    }
}
