use e2e::*;
use std::time::Instant;

#[test]
fn test_update_performance() {
    eprintln!("\n=== Test: UPDATE Performance ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    eprintln!("[Perf] Creating table...");
    let start = Instant::now();
    db.execute("CREATE TABLE test_items (id INT, name TEXT, price INT, status INT)").unwrap();
    eprintln!("[Perf] CREATE TABLE: {:?}", start.elapsed());

    eprintln!("[Perf] Inserting 10 rows...");
    let start = Instant::now();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO test_items VALUES ({}, 'Item{}', {}, 1)", i, i, i * 100))
            .unwrap();
    }
    eprintln!("[Perf] 10 INSERTs: {:?}", start.elapsed());

    eprintln!("[Perf] Running UPDATE with WHERE clause...");
    let start = Instant::now();
    let result =
        db.execute("UPDATE test_items SET status = 0, price = 999 WHERE id = 1 AND status = 1");
    let update1_time = start.elapsed();
    eprintln!("[Perf] UPDATE #1: {:?}", update1_time);
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);

    eprintln!("[Perf] Running second UPDATE...");
    let start = Instant::now();
    let result = db.execute("UPDATE test_items SET status = 0 WHERE id = 2 AND status = 1");
    let update2_time = start.elapsed();
    eprintln!("[Perf] UPDATE #2: {:?}", update2_time);
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);

    eprintln!("[Perf] Running third UPDATE...");
    let start = Instant::now();
    let result = db.execute("UPDATE test_items SET price = 1500 WHERE id = 3");
    let update3_time = start.elapsed();
    eprintln!("[Perf] UPDATE #3: {:?}", update3_time);
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);

    eprintln!("[Perf] Verifying results...");
    let start = Instant::now();
    let result = db.execute("SELECT * FROM test_items WHERE status = 0");
    eprintln!("[Perf] SELECT: {:?}", start.elapsed());
    assert!(result.is_ok());

    if update1_time.as_secs() > 5 {
        eprintln!("[WARNING] UPDATE #1 took {:?} - investigating slow performance", update1_time);
    }
    if update2_time.as_secs() > 5 {
        eprintln!("[WARNING] UPDATE #2 took {:?} - investigating slow performance", update2_time);
    }
    if update3_time.as_secs() > 5 {
        eprintln!("[WARNING] UPDATE #3 took {:?} - investigating slow performance", update3_time);
    }

    assert!(update1_time.as_millis() < 500, "UPDATE #1 too slow: {:?}", update1_time);
    assert!(update2_time.as_millis() < 500, "UPDATE #2 too slow: {:?}", update2_time);
    assert!(update3_time.as_millis() < 500, "UPDATE #3 too slow: {:?}", update3_time);

    db.execute("DROP TABLE test_items").ok();
    eprintln!("=== Test PASSED ===");
}
