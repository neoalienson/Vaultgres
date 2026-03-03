use e2e::*;
use std::thread;
use std::time::Instant;

mod pet_store;
mod referential_integrity;

#[test]
fn test_oltp_simple_transactions() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE accounts (id INT, balance INT)").unwrap();
    db.execute("INSERT INTO accounts VALUES (1, 1000)").unwrap();
    db.execute("INSERT INTO accounts VALUES (2, 2000)").unwrap();

    let start = Instant::now();
    let mut errors = 0;

    for _ in 0..100 {
        if db.execute("UPDATE accounts SET balance = balance - 10 WHERE id = 1").is_err() {
            errors += 1;
        }
    }

    let duration = start.elapsed();
    let qps = 100.0 / duration.as_secs_f64();

    assert!(qps > 5.0, "QPS too low: {}", qps);
    assert!(errors < 10, "Too many errors: {}", errors);
}

#[test]
fn test_oltp_concurrent_inserts() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (id INT, product TEXT, amount INT)").unwrap();

    let handles: Vec<_> = (0..10).map(|thread_id| {
        thread::spawn(move || {
            let db = DbConnection::connect("localhost", 5432);
            for i in 0..100 {
                let sql = format!(
                    "INSERT INTO orders VALUES ({}, 'product{}', {})",
                    thread_id * 100 + i, i, i * 10
                );
                db.execute(&sql).ok();
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    let count: i32 = db.query_scalar("SELECT COUNT(*) FROM orders");
    assert!(count >= 900, "Expected ~1000 rows, got {}", count);
}

#[test]
fn test_oltp_read_heavy_workload() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE products (id INT, name TEXT, price INT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO products VALUES ({}, 'product{}', {})", i, i, i * 10)).unwrap();
    }

    let start = Instant::now();
    for i in 0..500 {
        db.execute(&format!("SELECT * FROM products WHERE id = {}", i % 1000)).ok();
    }
    let duration = start.elapsed();

    assert!(duration.as_secs() < 10, "Read workload too slow: {:?}", duration);
}
