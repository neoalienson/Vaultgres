use rustgres::optimizer::{LogicalPlan, RuleOptimizer, OptimizationRule, PredicatePushdown, ProjectionPruning, ConstantFolding};
use rustgres::parser::{Expr, BinaryOperator};

#[test]
fn test_predicate_pushdown_to_scan() {
    let rule = PredicatePushdown;
    let scan = LogicalPlan::scan("users".to_string());
    let filter = LogicalPlan::filter(scan, Expr::Number(1));
    
    let optimized = rule.apply(filter);
    match optimized {
        LogicalPlan::Scan { filter, .. } => assert!(filter.is_some()),
        _ => panic!("Expected scan with filter"),
    }
}

#[test]
fn test_projection_pruning() {
    let rule = ProjectionPruning;
    let scan = LogicalPlan::scan("users".to_string());
    let proj1 = LogicalPlan::project(scan, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    let proj2 = LogicalPlan::project(proj1, vec!["a".to_string()]);
    
    let optimized = rule.apply(proj2);
    match optimized {
        LogicalPlan::Project { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected project"),
    }
}

#[test]
fn test_constant_folding_equality() {
    let rule = ConstantFolding;
    let scan = LogicalPlan::scan("users".to_string());
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(10)),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(10)),
    };
    let filter = LogicalPlan::filter(scan, expr);
    
    let optimized = rule.apply(filter);
    match optimized {
        LogicalPlan::Filter { predicate, .. } => {
            match predicate {
                Expr::Number(1) => (),
                _ => panic!("Expected folded number 1"),
            }
        }
        _ => panic!("Expected filter"),
    }
}

#[test]
fn test_constant_folding_inequality() {
    let rule = ConstantFolding;
    let scan = LogicalPlan::scan("users".to_string());
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(5)),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(10)),
    };
    let filter = LogicalPlan::filter(scan, expr);
    
    let optimized = rule.apply(filter);
    match optimized {
        LogicalPlan::Filter { predicate, .. } => {
            match predicate {
                Expr::Number(0) => (),
                _ => panic!("Expected folded number 0"),
            }
        }
        _ => panic!("Expected filter"),
    }
}

#[test]
fn test_rule_optimizer_default() {
    let optimizer = RuleOptimizer::default();
    let scan = LogicalPlan::scan("users".to_string());
    let filter = LogicalPlan::filter(scan, Expr::Number(1));
    
    let optimized = optimizer.optimize(filter);
    match optimized {
        LogicalPlan::Scan { filter, .. } => assert!(filter.is_some()),
        _ => panic!("Expected optimized scan"),
    }
}

#[test]
fn test_multiple_rules_applied() {
    let optimizer = RuleOptimizer::default();
    let scan = LogicalPlan::scan("users".to_string());
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Number(2)),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(2)),
    };
    let filter = LogicalPlan::filter(scan, expr);
    let proj = LogicalPlan::project(filter, vec!["id".to_string()]);
    
    let optimized = optimizer.optimize(proj);
    assert!(matches!(optimized, LogicalPlan::Project { .. }));
}

#[test]
fn test_nested_plan_optimization() {
    let optimizer = RuleOptimizer::default();
    let scan1 = LogicalPlan::scan("t1".to_string());
    let scan2 = LogicalPlan::scan("t2".to_string());
    let join = LogicalPlan::join(scan1, scan2);
    let filter = LogicalPlan::filter(join, Expr::Number(1));
    
    let optimized = optimizer.optimize(filter);
    assert!(matches!(optimized, LogicalPlan::Join { .. }));
}
