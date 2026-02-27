use super::{OptimizationRule, LogicalPlan};
use crate::parser::{Expr, BinaryOperator};

pub struct ConstantFolding;

impl ConstantFolding {
    fn fold_expr(&self, expr: Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = self.fold_expr(*left);
                let right = self.fold_expr(*right);
                
                match (&left, &right, op) {
                    (Expr::Number(l), Expr::Number(r), BinaryOperator::Equals) => {
                        Expr::Number(if l == r { 1 } else { 0 })
                    }
                    _ => Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) }
                }
            }
            other => other,
        }
    }
}

impl OptimizationRule for ConstantFolding {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                LogicalPlan::Filter {
                    input: Box::new(self.apply(*input)),
                    predicate: self.fold_expr(predicate),
                }
            }
            LogicalPlan::Project { input, columns } => {
                LogicalPlan::Project {
                    input: Box::new(self.apply(*input)),
                    columns,
                }
            }
            LogicalPlan::Join { left, right, condition } => {
                LogicalPlan::Join {
                    left: Box::new(self.apply(*left)),
                    right: Box::new(self.apply(*right)),
                    condition: condition.map(|c| self.fold_expr(c)),
                }
            }
            other => other,
        }
    }
    
    fn name(&self) -> &str {
        "ConstantFolding"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::BinaryOperator;
    
    #[test]
    fn test_fold_equality() {
        let rule = ConstantFolding;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(5)),
        };
        
        let folded = rule.fold_expr(expr);
        match folded {
            Expr::Number(1) => (),
            _ => panic!("Expected number 1"),
        }
    }
}
