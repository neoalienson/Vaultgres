use rustgres::catalog::{CheckValidator, Tuple, Value};
use rustgres::parser::ast::{BinaryOperator, CheckConstraint, Expr};

#[test]
fn test_check_constraint_age_validation() {
    let constraint = CheckConstraint {
        name: Some("age_positive".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(0)),
        },
    };

    let mut valid_tuple = Tuple::new();
    valid_tuple.add_value("age".to_string(), Value::Int(25));
    assert!(CheckValidator::validate(&constraint, &valid_tuple).is_ok());

    let mut invalid_tuple = Tuple::new();
    invalid_tuple.add_value("age".to_string(), Value::Int(-5));
    assert!(CheckValidator::validate(&constraint, &invalid_tuple).is_err());
}

#[test]
fn test_check_constraint_range_validation() {
    let min_constraint = CheckConstraint {
        name: Some("min_age".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(18)),
        },
    };

    let max_constraint = CheckConstraint {
        name: Some("max_age".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(Expr::Number(65)),
        },
    };

    let mut valid_tuple = Tuple::new();
    valid_tuple.add_value("age".to_string(), Value::Int(30));
    assert!(CheckValidator::validate(&min_constraint, &valid_tuple).is_ok());
    assert!(CheckValidator::validate(&max_constraint, &valid_tuple).is_ok());

    let mut too_young = Tuple::new();
    too_young.add_value("age".to_string(), Value::Int(17));
    assert!(CheckValidator::validate(&min_constraint, &too_young).is_err());

    let mut too_old = Tuple::new();
    too_old.add_value("age".to_string(), Value::Int(66));
    assert!(CheckValidator::validate(&max_constraint, &too_old).is_err());
}

#[test]
fn test_check_constraint_text_validation() {
    let constraint = CheckConstraint {
        name: Some("status_check".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("active".to_string())),
        },
    };

    let mut valid_tuple = Tuple::new();
    valid_tuple.add_value("status".to_string(), Value::Text("active".to_string()));
    assert!(CheckValidator::validate(&constraint, &valid_tuple).is_ok());

    let mut invalid_tuple = Tuple::new();
    invalid_tuple.add_value("status".to_string(), Value::Text("inactive".to_string()));
    assert!(CheckValidator::validate(&constraint, &invalid_tuple).is_err());
}

#[test]
fn test_check_constraint_multiple_columns() {
    let constraint = CheckConstraint {
        name: Some("salary_check".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("salary".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(0)),
        },
    };

    let mut tuple = Tuple::new();
    tuple.add_value("name".to_string(), Value::Text("Alice".to_string()));
    tuple.add_value("salary".to_string(), Value::Int(50000));
    tuple.add_value("department".to_string(), Value::Text("Engineering".to_string()));

    assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
}

#[test]
fn test_check_constraint_error_message() {
    let constraint = CheckConstraint {
        name: Some("custom_check".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(100)),
        },
    };

    let mut tuple = Tuple::new();
    tuple.add_value("value".to_string(), Value::Int(50));

    let result = CheckValidator::validate(&constraint, &tuple);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.contains("custom_check"));
    assert!(error.contains("violated"));
}

#[test]
fn test_check_constraint_boundary_conditions() {
    let constraint = CheckConstraint {
        name: Some("boundary_check".to_string()),
        expr: Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(0)),
        },
    };

    let mut zero_tuple = Tuple::new();
    zero_tuple.add_value("value".to_string(), Value::Int(0));
    assert!(CheckValidator::validate(&constraint, &zero_tuple).is_ok());

    let mut negative_tuple = Tuple::new();
    negative_tuple.add_value("value".to_string(), Value::Int(-1));
    assert!(CheckValidator::validate(&constraint, &negative_tuple).is_err());
}
