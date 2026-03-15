// Pet Store Persistence Tests
use e2e::*;

pub fn run_persistence_tests(env: &RunningEnv) {
    eprintln!("\n[PetStore] === Testing Persistence Across Restarts ===");
    
    test_graceful_restart(env);
    test_kill_restart(env);
    test_stop_start_restart(env);
    test_multiple_rapid_restarts(env);
}

fn test_graceful_restart(env: &RunningEnv) {
    eprintln!("\n[PetStore] === Testing Graceful Restart (SIGTERM) ===");
    eprintln!("[PetStore] Stopping server for persistence test...");
    env.restart_graceful(5);
    let db = env.vaultgres();

    eprintln!("[PetStore] Verifying data persistence after graceful restart...");
    let result = db.execute("SELECT * FROM items");
    assert!(result.is_ok(), "Items table should persist after graceful restart");
    let output = result.unwrap();
    assert!(output.contains("Dog Food"), "Dog Food should persist");
    assert!(output.contains("Catnip"), "Catnip should persist");

    eprintln!("[PetStore] Verifying SCD Type 2 persistence...");
    let result = db.execute("SELECT * FROM items WHERE item_id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("4500"), "Historical price should persist");
    assert!(output.contains("4799"), "Current price should persist");

    eprintln!("[PetStore] Verifying index persistence...");
    let result = db.execute("SELECT * FROM items WHERE category = 'Food'");
    assert!(result.is_ok(), "Index query should work after restart");

    eprintln!("[PetStore] Verifying view persistence...");
    let result = db.execute("SELECT * FROM current_items");
    assert!(result.is_ok(), "View should persist after restart");

    eprintln!("[PetStore] Verifying materialized view persistence...");
    let result = db.execute("SELECT * FROM category_sales");
    assert!(result.is_ok(), "Materialized view should persist after restart");

    eprintln!("[PetStore] Verifying orders persistence...");
    let result = db.execute("SELECT * FROM orders");
    assert!(result.is_ok(), "Orders should persist after restart");

    eprintln!("[PetStore] Verifying inventory persistence...");
    let result = db.execute("SELECT * FROM inventory");
    assert!(result.is_ok(), "Inventory should persist after restart");
}

fn test_kill_restart(env: &RunningEnv) {
    eprintln!("\n[PetStore] === Testing Kill Restart (SIGKILL - Crash Simulation) ===");
    env.restart_with_kill(5);
    let db = env.vaultgres();

    eprintln!("[PetStore] Verifying data persistence after kill...");
    let result = db.execute("SELECT COUNT(*) FROM items");
    assert!(result.is_ok(), "Items table should persist after kill");

    let result = db.execute("SELECT COUNT(*) FROM customers");
    assert!(result.is_ok(), "Customers table should persist after kill");

    let result = db.execute("SELECT COUNT(*) FROM orders");
    assert!(result.is_ok(), "Orders table should persist after kill");
}

fn test_stop_start_restart(env: &RunningEnv) {
    eprintln!("\n[PetStore] === Testing Stop/Start Restart ===");
    env.restart_with_stop_start(5);
    let db = env.vaultgres();

    eprintln!("[PetStore] Verifying data persistence after stop/start...");
    let result = db.execute("SELECT COUNT(*) FROM items");
    assert!(result.is_ok(), "Items table should persist after stop/start");

    eprintln!("[PetStore] Verifying JOIN queries after restart...");
    let result = db.execute("SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id");
    assert!(result.is_ok(), "JOIN queries should work after restart");
}

fn test_multiple_rapid_restarts(env: &RunningEnv) {
    eprintln!("\n[PetStore] === Testing Multiple Rapid Restarts ===");
    for i in 0..3 {
        eprintln!("[PetStore] Rapid restart iteration {}", i + 1);
        env.restart_graceful(3);
        let db = env.vaultgres();

        let result = db.execute("SELECT COUNT(*) FROM items");
        assert!(result.is_ok(), "Data should persist through rapid restart {}", i + 1);
    }
}
