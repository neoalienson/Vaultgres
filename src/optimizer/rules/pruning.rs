use super::{OptimizationRule, LogicalPlan};

pub struct ProjectionPruning;

impl OptimizationRule for ProjectionPruning {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Project { input, columns } => {
                match *input {
                    LogicalPlan::Project { input: inner, columns: inner_cols } => {
                        let pruned: Vec<String> = inner_cols.into_iter()
                            .filter(|c| columns.contains(c))
                            .collect();
                        LogicalPlan::Project {
                            input: Box::new(self.apply(*inner)),
                            columns: if pruned.is_empty() { columns } else { pruned },
                        }
                    }
                    other => LogicalPlan::Project {
                        input: Box::new(self.apply(other)),
                        columns,
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                LogicalPlan::Filter {
                    input: Box::new(self.apply(*input)),
                    predicate,
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
        "ProjectionPruning"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_prune_nested_projections() {
        let rule = ProjectionPruning;
        let scan = LogicalPlan::scan("users".to_string());
        let proj1 = LogicalPlan::project(scan, vec!["a".to_string(), "b".to_string()]);
        let proj2 = LogicalPlan::project(proj1, vec!["a".to_string()]);
        
        let optimized = rule.apply(proj2);
        match optimized {
            LogicalPlan::Project { columns, .. } => assert_eq!(columns.len(), 1),
            _ => panic!("Expected project"),
        }
    }
}
