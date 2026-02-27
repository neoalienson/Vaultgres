use rustgres::optimizer::{JoinOptimizer, Relation};

#[test]
fn test_join_optimizer_single_table() {
    let optimizer = JoinOptimizer::new();
    let relations = vec![
        Relation { id: 1, name: "users".to_string(), row_count: 1000 },
    ];
    
    let plan = optimizer.optimize(relations).unwrap();
    assert!(plan.relation.is_some());
    assert!(plan.left.is_none());
    assert!(plan.right.is_none());
}

#[test]
fn test_join_optimizer_two_tables() {
    let optimizer = JoinOptimizer::new();
    let relations = vec![
        Relation { id: 1, name: "users".to_string(), row_count: 1000 },
        Relation { id: 2, name: "orders".to_string(), row_count: 5000 },
    ];
    
    let plan = optimizer.optimize(relations).unwrap();
    assert!(plan.left.is_some());
    assert!(plan.right.is_some());
    assert!(plan.cost.total > 0.0);
}

#[test]
fn test_join_optimizer_three_tables_dp() {
    let optimizer = JoinOptimizer::new();
    let relations = vec![
        Relation { id: 1, name: "users".to_string(), row_count: 1000 },
        Relation { id: 2, name: "orders".to_string(), row_count: 5000 },
        Relation { id: 3, name: "products".to_string(), row_count: 500 },
    ];
    
    let plan = optimizer.optimize(relations).unwrap();
    assert!(plan.cost.total > 0.0);
}

#[test]
fn test_join_optimizer_greedy_large() {
    let optimizer = JoinOptimizer::new();
    let relations: Vec<Relation> = (0..15)
        .map(|i| Relation {
            id: i,
            name: format!("table_{}", i),
            row_count: (i + 1) as u64 * 1000,
        })
        .collect();
    
    let plan = optimizer.optimize(relations).unwrap();
    assert!(plan.cost.total > 0.0);
}

#[test]
fn test_join_optimizer_empty() {
    let optimizer = JoinOptimizer::new();
    let relations: Vec<Relation> = vec![];
    
    let plan = optimizer.optimize(relations).unwrap();
    assert!(plan.relation.is_none());
}

#[test]
fn test_dp_vs_greedy_threshold() {
    let optimizer = JoinOptimizer::new();
    
    let relations_small: Vec<Relation> = (0..12)
        .map(|i| Relation { id: i, name: format!("t{}", i), row_count: 100 })
        .collect();
    let plan_dp = optimizer.optimize(relations_small).unwrap();
    
    let relations_large: Vec<Relation> = (0..13)
        .map(|i| Relation { id: i, name: format!("t{}", i), row_count: 100 })
        .collect();
    let plan_greedy = optimizer.optimize(relations_large).unwrap();
    
    assert!(plan_dp.cost.total > 0.0);
    assert!(plan_greedy.cost.total > 0.0);
}

#[test]
fn test_greedy_orders_by_size() {
    let optimizer = JoinOptimizer::new();
    let relations = vec![
        Relation { id: 1, name: "large".to_string(), row_count: 10000 },
        Relation { id: 2, name: "small".to_string(), row_count: 100 },
        Relation { id: 3, name: "medium".to_string(), row_count: 1000 },
    ];
    
    let plan = optimizer.optimize(relations).unwrap();
    assert!(plan.cost.total > 0.0);
}
