use e2e::*;
use std::thread;
use std::time::Duration;

#[test]
#[ignore]
fn test_memory_leak_24h() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_monitoring()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE events (id INT, data TEXT, ts TIMESTAMP)").unwrap();
    
    let monitor = env.start_monitor();
    
    for i in 0..86400 {
        db.execute(&format!(
            "INSERT INTO events VALUES ({}, 'data{}', NOW())",
            i, i
        )).ok();
        
        if i % 100 == 0 {
            db.execute("SELECT COUNT(*) FROM events").ok();
        }
        
        thread::sleep(Duration::from_secs(1));
    }
    
    let metrics = monitor.stop();
    assert!(metrics.memory_growth_mb < 100.0, "Memory leak detected: {} MB growth", metrics.memory_growth_mb);
}

#[test]
#[ignore]
fn test_connection_churn_12h() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_monitoring()
        .start();
    
    let monitor = env.start_monitor();
    
    for _ in 0..43200 {
        let db = DbConnection::connect("localhost", 5432);
        db.execute("SELECT 1").ok();
        thread::sleep(Duration::from_secs(1));
    }
    
    let metrics = monitor.stop();
    assert!(metrics.memory_growth_mb < 50.0, "Connection leak: {} MB", metrics.memory_growth_mb);
}

#[test]
#[ignore]
fn test_disk_growth_48h() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE logs (id INT, message TEXT)").unwrap();
    
    for hour in 0..48 {
        for i in 0..3600 {
            db.execute(&format!("INSERT INTO logs VALUES ({}, 'log message {}')", hour * 3600 + i, i)).ok();
        }
        thread::sleep(Duration::from_secs(3600));
    }
}
