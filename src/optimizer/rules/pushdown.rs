use super::{OptimizationRule, LogicalPlan};

pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                match *input {
                    LogicalPlan::Scan { table, filter, columns } => {
                        LogicalPlan::Scan {
                            table,
                            filter: Some(predicate),
                            columns,
                        }
                    }
                    LogicalPlan::Join { left, right, condition } => {
                        LogicalPlan::Join {
                            left: Box::new(LogicalPlan::Filter { input: left, predicate: predicate.clone() }),
                            right,
                            condition,
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(self.apply(other)),
                        predicate,
                    }
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
                    condition,
                }
            }
            other => other,
        }
    }
    
    fn name(&self) -> &str {
        "PredicatePushdown"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Expr;
    
    #[test]
    fn test_pushdown_to_scan() {
        let rule = PredicatePushdown;
        let scan = LogicalPlan::scan("users".to_string());
        let filter = LogicalPlan::filter(scan, Expr::Number(1));
        
        let optimized = rule.apply(filter);
        match optimized {
            LogicalPlan::Scan { filter, .. } => assert!(filter.is_some()),
            _ => panic!("Expected scan with filter"),
        }
    }
}
