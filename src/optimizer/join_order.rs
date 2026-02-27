use super::cost::{Cost, CostModel};
use super::error::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Relation {
    pub id: usize,
    pub name: String,
    pub row_count: u64,
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub left: Option<Box<JoinPlan>>,
    pub right: Option<Box<JoinPlan>>,
    pub relation: Option<Relation>,
    pub cost: Cost,
}

pub struct JoinOptimizer {
    cost_model: CostModel,
}

impl JoinOptimizer {
    pub fn new() -> Self {
        Self {
            cost_model: CostModel::new(),
        }
    }
    
    pub fn optimize(&self, relations: Vec<Relation>) -> Result<JoinPlan> {
        if relations.is_empty() {
            return Ok(JoinPlan {
                left: None,
                right: None,
                relation: None,
                cost: Cost::zero(),
            });
        }
        
        if relations.len() == 1 {
            return Ok(JoinPlan {
                left: None,
                right: None,
                relation: Some(relations[0].clone()),
                cost: Cost::new(0.0, relations[0].row_count as f64, relations[0].row_count as f64),
            });
        }
        
        if relations.len() <= 12 {
            self.optimize_dp(relations)
        } else {
            self.optimize_greedy(relations)
        }
    }
    
    fn optimize_dp(&self, relations: Vec<Relation>) -> Result<JoinPlan> {
        let n = relations.len();
        let mut dp: HashMap<Vec<usize>, JoinPlan> = HashMap::new();
        
        for (i, rel) in relations.iter().enumerate() {
            let plan = JoinPlan {
                left: None,
                right: None,
                relation: Some(rel.clone()),
                cost: Cost::new(0.0, rel.row_count as f64, rel.row_count as f64),
            };
            dp.insert(vec![i], plan);
        }
        
        for size in 2..=n {
            let subsets = self.generate_subsets(&relations, size);
            for subset in subsets {
                let mut best_plan: Option<JoinPlan> = None;
                let mut best_cost = f64::MAX;
                
                for split in 1..subset.len() {
                    let left_set: Vec<usize> = subset[..split].to_vec();
                    let right_set: Vec<usize> = subset[split..].to_vec();
                    
                    if let (Some(left_plan), Some(right_plan)) = (dp.get(&left_set), dp.get(&right_set)) {
                        let join_cost = self.cost_model.estimate_nested_loop_join(&left_plan.cost, &right_plan.cost)?;
                        
                        if join_cost.total < best_cost {
                            best_cost = join_cost.total;
                            best_plan = Some(JoinPlan {
                                left: Some(Box::new(left_plan.clone())),
                                right: Some(Box::new(right_plan.clone())),
                                relation: None,
                                cost: join_cost,
                            });
                        }
                    }
                }
                
                if let Some(plan) = best_plan {
                    dp.insert(subset, plan);
                }
            }
        }
        
        let all_indices: Vec<usize> = (0..n).collect();
        Ok(dp.remove(&all_indices).unwrap())
    }
    
    fn optimize_greedy(&self, mut relations: Vec<Relation>) -> Result<JoinPlan> {
        relations.sort_by_key(|r| r.row_count);
        
        let mut plan = JoinPlan {
            left: None,
            right: None,
            relation: Some(relations[0].clone()),
            cost: Cost::new(0.0, relations[0].row_count as f64, relations[0].row_count as f64),
        };
        
        for rel in relations.iter().skip(1) {
            let right_plan = JoinPlan {
                left: None,
                right: None,
                relation: Some(rel.clone()),
                cost: Cost::new(0.0, rel.row_count as f64, rel.row_count as f64),
            };
            
            let join_cost = self.cost_model.estimate_nested_loop_join(&plan.cost, &right_plan.cost)?;
            
            plan = JoinPlan {
                left: Some(Box::new(plan)),
                right: Some(Box::new(right_plan)),
                relation: None,
                cost: join_cost,
            };
        }
        
        Ok(plan)
    }
    
    fn generate_subsets(&self, relations: &[Relation], size: usize) -> Vec<Vec<usize>> {
        let n = relations.len();
        let mut result = Vec::new();
        let mut current = Vec::new();
        self.generate_subsets_helper(n, size, 0, &mut current, &mut result);
        result
    }
    
    fn generate_subsets_helper(&self, n: usize, size: usize, start: usize, current: &mut Vec<usize>, result: &mut Vec<Vec<usize>>) {
        if current.len() == size {
            result.push(current.clone());
            return;
        }
        
        for i in start..n {
            current.push(i);
            self.generate_subsets_helper(n, size, i + 1, current, result);
            current.pop();
        }
    }
}

impl Default for JoinOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_single_relation() {
        let optimizer = JoinOptimizer::new();
        let relations = vec![Relation { id: 1, name: "t1".to_string(), row_count: 100 }];
        let plan = optimizer.optimize(relations).unwrap();
        assert!(plan.relation.is_some());
    }
    
    #[test]
    fn test_two_relations_dp() {
        let optimizer = JoinOptimizer::new();
        let relations = vec![
            Relation { id: 1, name: "t1".to_string(), row_count: 100 },
            Relation { id: 2, name: "t2".to_string(), row_count: 200 },
        ];
        let plan = optimizer.optimize(relations).unwrap();
        assert!(plan.left.is_some());
        assert!(plan.right.is_some());
    }
    
    #[test]
    fn test_greedy_many_relations() {
        let optimizer = JoinOptimizer::new();
        let relations: Vec<Relation> = (0..15)
            .map(|i| Relation { id: i, name: format!("t{}", i), row_count: (i + 1) as u64 * 100 })
            .collect();
        let plan = optimizer.optimize(relations).unwrap();
        assert!(plan.cost.total > 0.0);
    }
}
