use e2e::*;

#[test]
fn test_crash_recovery_basic() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE crash_test (id INT, data TEXT)").unwrap();
    db.execute("INSERT INTO crash_test VALUES (1, 'before crash')").unwrap();
    
    env.kill_container();
    env.restart();
    
    let result = db.execute("SELECT * FROM crash_test").unwrap();
    assert!(result.contains("before crash"), "Data lost after crash");
}

#[test]
fn test_wal_recovery() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE wal_test (id INT)").unwrap();
    
    for i in 0..1000 {
        db.execute(&format!("INSERT INTO wal_test VALUES ({})", i)).unwrap();
    }
    
    env.kill_container();
    env.restart();
    
    let count: i32 = db.query_scalar("SELECT COUNT(*) FROM wal_test");
    assert_eq!(count, 1000, "WAL recovery incomplete");
}

#[test]
fn test_multiple_crash_recovery() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE multi_crash (id INT, value INT)").unwrap();
    
    for cycle in 0..5 {
        db.execute(&format!("INSERT INTO multi_crash VALUES ({}, {})", cycle, cycle * 100)).unwrap();
        env.kill_container();
        env.restart();
    }
    
    let count: i32 = db.query_scalar("SELECT COUNT(*) FROM multi_crash");
    assert_eq!(count, 5, "Data lost across multiple crashes");
}
