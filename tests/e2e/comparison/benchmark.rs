use e2e::*;

#[test]
fn test_compare_simple_select() {
    let env = TestEnv::new().with_vaultgres().with_postgres().start();
    
    for db in [env.vaultgres(), env.postgres()] {
        db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
        for i in 0..10000 {
            db.execute(&format!("INSERT INTO users VALUES ({}, 'user{}')", i, i)).unwrap();
        }
    }

    let vaultgres_time = env.vaultgres().time_query("SELECT * FROM users WHERE id = 5000");
    let postgres_time = env.postgres().time_query("SELECT * FROM users WHERE id = 5000");

    let speedup = postgres_time.as_secs_f64() / vaultgres_time.as_secs_f64();
    println!("VaultGres: {:?}, Postgres: {:?}, Speedup: {:.2}x", vaultgres_time, postgres_time, speedup);
    
    assert!(speedup > 0.5, "VaultGres significantly slower");
}

#[test]
fn test_compare_join_performance() {
    let env = TestEnv::new().with_vaultgres().with_postgres().start();
    
    for db in [env.vaultgres(), env.postgres()] {
        db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)").unwrap();
        db.execute("CREATE TABLE customers (id INT, name TEXT)").unwrap();
        
        for i in 0..1000 {
            db.execute(&format!("INSERT INTO customers VALUES ({}, 'customer{}')", i, i)).unwrap();
            db.execute(&format!("INSERT INTO orders VALUES ({}, {}, {})", i, i % 100, i * 10)).unwrap();
        }
    }

    let query = "SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name";
    
    let vaultgres_time = env.vaultgres().time_query(query);
    let postgres_time = env.postgres().time_query(query);

    let speedup = postgres_time.as_secs_f64() / vaultgres_time.as_secs_f64();
    println!("Join - VaultGres: {:?}, Postgres: {:?}, Speedup: {:.2}x", vaultgres_time, postgres_time, speedup);
}

#[test]
fn test_compare_aggregation() {
    let env = TestEnv::new().with_vaultgres().with_postgres().start();
    
    for db in [env.vaultgres(), env.postgres()] {
        db.execute("CREATE TABLE sales (product_id INT, amount INT, region TEXT)").unwrap();
        for i in 0..5000 {
            db.execute(&format!("INSERT INTO sales VALUES ({}, {}, 'region{}')", i % 100, i, i % 10)).unwrap();
        }
    }

    let query = "SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY region";
    
    let vaultgres_time = env.vaultgres().time_query(query);
    let postgres_time = env.postgres().time_query(query);

    let speedup = postgres_time.as_secs_f64() / vaultgres_time.as_secs_f64();
    println!("Aggregation - VaultGres: {:?}, Postgres: {:?}, Speedup: {:.2}x", vaultgres_time, postgres_time, speedup);
}
