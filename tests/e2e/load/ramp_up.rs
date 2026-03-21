use e2e::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

#[test]
#[ignore]
fn test_ramp_up_load() {
    let env = TestEnv::new().with_vaultgres().with_monitoring().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE load_test (id INT, value INT)").unwrap();
    for i in 0..1000 {
        db.execute(&format!("INSERT INTO load_test VALUES ({}, {})", i, i)).unwrap();
    }

    let queries = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    for users in (10..=100).step_by(10) {
        println!("Testing with {} concurrent users", users);

        let handles: Vec<_> = (0..users)
            .map(|_| {
                let q = queries.clone();
                let e = errors.clone();
                thread::spawn(move || {
                    let db = DbConnection::connect("localhost", 5432);
                    for _ in 0..100 {
                        if db.execute("SELECT * FROM load_test WHERE id = 500").is_ok() {
                            q.fetch_add(1, Ordering::Relaxed);
                        } else {
                            e.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        thread::sleep(Duration::from_secs(5));
    }

    let total_queries = queries.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    let error_rate = total_errors as f64 / total_queries as f64;

    assert!(error_rate < 0.01, "Error rate too high: {:.2}%", error_rate * 100.0);
}

#[test]
#[ignore]
fn test_spike_load() {
    let env = TestEnv::new().with_vaultgres().with_monitoring().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE spike_test (id INT)").unwrap();

    let baseline_users = 20;
    let spike_users = 200;

    println!("Baseline load: {} users", baseline_users);
    run_load(baseline_users, 60);

    println!("SPIKE: {} users", spike_users);
    let start = Instant::now();
    run_load(spike_users, 120);
    let spike_duration = start.elapsed();

    println!("Recovery: {} users", baseline_users);
    run_load(baseline_users, 60);

    assert!(spike_duration.as_secs() < 180, "Spike handling too slow");
}

fn run_load(users: usize, duration_secs: u64) {
    let handles: Vec<_> = (0..users)
        .map(|_| {
            thread::spawn(move || {
                let db = DbConnection::connect("localhost", 5432);
                let end = Instant::now() + Duration::from_secs(duration_secs);
                while Instant::now() < end {
                    db.execute("SELECT 1").ok();
                    thread::sleep(Duration::from_millis(100));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
