use vaultgres::optimizer::{LogicalPlan, PlanCache};

#[test]
fn test_plan_cache_basic() {
    let cache = PlanCache::new(10);
    let sql = "SELECT * FROM users WHERE id = 1";
    let plan = LogicalPlan::Scan { table: "users".to_string(), filter: None, columns: vec![] };

    cache.insert(sql.to_string(), plan.clone());
    let cached = cache.get(sql);

    assert!(cached.is_some());
}

#[test]
fn test_plan_cache_multiple_queries() {
    let cache = PlanCache::new(100);

    for i in 0..10 {
        let sql = format!("SELECT * FROM table{}", i);
        let plan =
            LogicalPlan::Scan { table: format!("table{}", i), filter: None, columns: vec![] };
        cache.insert(sql.clone(), plan);
    }

    assert_eq!(cache.len(), 10);

    for i in 0..10 {
        let sql = format!("SELECT * FROM table{}", i);
        assert!(cache.get(&sql).is_some());
    }
}

#[test]
fn test_plan_cache_eviction_policy() {
    let cache = PlanCache::new(3);

    let plan1 = LogicalPlan::Scan { table: "t1".to_string(), filter: None, columns: vec![] };
    let plan2 = LogicalPlan::Scan { table: "t2".to_string(), filter: None, columns: vec![] };
    let plan3 = LogicalPlan::Scan { table: "t3".to_string(), filter: None, columns: vec![] };

    cache.insert("q1".to_string(), plan1);
    cache.insert("q2".to_string(), plan2);
    cache.insert("q3".to_string(), plan3);

    // Access q1 and q2 to increase hit count
    cache.get("q1");
    cache.get("q2");

    // Insert q4, should evict q3 (lowest hit count)
    let plan4 = LogicalPlan::Scan { table: "t4".to_string(), filter: None, columns: vec![] };
    cache.insert("q4".to_string(), plan4);

    assert_eq!(cache.len(), 3);
    assert!(cache.get("q3").is_none());
    assert!(cache.get("q1").is_some());
    assert!(cache.get("q2").is_some());
    assert!(cache.get("q4").is_some());
}

#[test]
fn test_plan_cache_hit_tracking() {
    let cache = PlanCache::new(10);
    let plan = LogicalPlan::Scan { table: "users".to_string(), filter: None, columns: vec![] };

    cache.insert("SELECT * FROM users".to_string(), plan);

    assert_eq!(cache.hit_count("SELECT * FROM users"), Some(0));

    cache.get("SELECT * FROM users");
    assert_eq!(cache.hit_count("SELECT * FROM users"), Some(1));

    cache.get("SELECT * FROM users");
    cache.get("SELECT * FROM users");
    assert_eq!(cache.hit_count("SELECT * FROM users"), Some(3));
}

#[test]
fn test_plan_cache_clear() {
    let cache = PlanCache::new(10);

    for i in 0..5 {
        let plan = LogicalPlan::Scan { table: format!("t{}", i), filter: None, columns: vec![] };
        cache.insert(format!("q{}", i), plan);
    }

    assert_eq!(cache.len(), 5);
    cache.clear();
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
}

#[test]
fn test_plan_cache_with_complex_plans() {
    let cache = PlanCache::new(10);

    let join_plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            filter: None,
            columns: vec![],
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "orders".to_string(),
            filter: None,
            columns: vec![],
        }),
        condition: None,
    };

    cache.insert("SELECT * FROM users JOIN orders".to_string(), join_plan);
    let cached = cache.get("SELECT * FROM users JOIN orders");

    assert!(cached.is_some());
}
