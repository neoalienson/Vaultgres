use crate::{DbConnection, RunningEnv, TestEnv};

#[test]
fn test_compression_basic() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS compressed_table").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE compressed_table (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create compressed table");

    db.execute("INSERT INTO compressed_table VALUES (1, 'hello')").expect("Failed to insert");

    let result = db.execute("SELECT * FROM compressed_table").expect("Failed to select");
    assert!(result.contains("hello"));

    env.cleanup();
}

#[test]
fn test_compression_zstd() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS zstd_table").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE zstd_table (id INT, large_data TEXT) WITH (compression_algorithm = 'zstd')",
    )
    .expect("Failed to create zstd table");

    db.execute("INSERT INTO zstd_table VALUES (1, 'test data')").expect("Failed to insert");

    let result = db.execute("SELECT * FROM zstd_table").expect("Failed to select");
    assert!(result.contains("test data"));

    env.cleanup();
}

#[test]
fn test_compression_large_values() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS large_data_table").expect("Failed to drop table");

    db.execute("CREATE TABLE large_data_table (id INT, large_text TEXT) WITH (compression_algorithm = 'lz4')")
        .expect("Failed to create table");

    let large_text = "A".repeat(5000);
    db.execute(&format!("INSERT INTO large_data_table VALUES (1, '{}')", large_text))
        .expect("Failed to insert large data");

    let result = db
        .execute("SELECT id, LENGTH(large_text) FROM large_data_table")
        .expect("Failed to select");
    assert!(result.contains("5000"));

    env.cleanup();
}

#[test]
fn test_compression_indexes() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS indexed_compressed").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE indexed_compressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    db.execute("CREATE INDEX idx_data ON indexed_compressed(data)")
        .expect("Failed to create index");

    db.execute("INSERT INTO indexed_compressed VALUES (1, 'indexed data')")
        .expect("Failed to insert");

    let result = db
        .execute("SELECT * FROM indexed_compressed WHERE data = 'indexed data'")
        .expect("Failed to select with index");
    assert!(result.contains("indexed data"));

    env.cleanup();
}

#[test]
fn test_compression_concurrent() {
    use std::thread;
    use std::time::Duration;

    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS concurrent_compressed").expect("Failed to drop table");

    db.execute("CREATE TABLE concurrent_compressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')")
        .expect("Failed to create table");

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let db_port = 5432;
            thread::spawn(move || {
                let conn = DbConnection::connect("localhost", db_port);
                for j in 0..10 {
                    let data = format!("data_{}_{}", i, j);
                    conn.execute(&format!(
                        "INSERT INTO concurrent_compressed VALUES ({}, '{}')",
                        i * 10 + j,
                        data
                    ))
                    .expect("Failed to insert");
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread failed");
    }

    thread::sleep(Duration::from_secs(1));

    let result = db.execute("SELECT COUNT(*) FROM concurrent_compressed").expect("Failed to count");
    assert!(result.contains("40"));

    env.cleanup();
}

#[test]
fn test_compression_persistence() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS persistent_compressed").expect("Failed to drop table");

    db.execute("CREATE TABLE persistent_compressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')")
        .expect("Failed to create table");

    db.execute("INSERT INTO persistent_compressed VALUES (1, 'persistent data')")
        .expect("Failed to insert");

    env.restart();

    let db = env.vaultgres();
    db.verify_connection();

    let result =
        db.execute("SELECT * FROM persistent_compressed").expect("Failed to select after restart");
    assert!(result.contains("persistent data"));

    env.cleanup();
}

#[test]
fn test_compression_algorithm_switch() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS algo_switch").expect("Failed to drop table");

    db.execute("CREATE TABLE algo_switch (id INT, data TEXT) WITH (compression_algorithm = 'lz4')")
        .expect("Failed to create table");

    db.execute("INSERT INTO algo_switch VALUES (1, 'initial data')").expect("Failed to insert");

    db.execute("ALTER TABLE algo_switch SET (compression_algorithm = 'zstd')")
        .expect("Failed to alter table");

    db.execute("INSERT INTO algo_switch VALUES (2, 'new data')")
        .expect("Failed to insert after alter");

    let result = db.execute("SELECT * FROM algo_switch ORDER BY id").expect("Failed to select");
    assert!(result.contains("initial data"));
    assert!(result.contains("new data"));

    env.cleanup();
}

#[test]
fn test_compression_edge_empty() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS empty_compressed").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE empty_compressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    let result =
        db.execute("SELECT COUNT(*) FROM empty_compressed").expect("Failed to count empty table");
    assert!(result.contains("0"));

    env.cleanup();
}

#[test]
fn test_compression_edge_empty_values() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS empty_values").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE empty_values (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO empty_values VALUES (1, '')").expect("Failed to insert empty string");

    let result = db.execute("SELECT * FROM empty_values").expect("Failed to select");
    assert!(result.contains("1"));

    env.cleanup();
}

#[test]
fn test_compression_existing_compressed() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS precompressed").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE precompressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    let already_compressed = vec![0u8; 10000];
    let data_str = String::from_utf8(already_compressed).unwrap();

    db.execute(&format!("INSERT INTO precompressed VALUES (1, '{}')", data_str))
        .expect("Failed to insert already compressed data");

    let result =
        db.execute("SELECT LENGTH(data) FROM precompressed").expect("Failed to get length");
    assert!(result.contains("10000"));

    env.cleanup();
}

#[test]
fn test_compression_no_compression_setting() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS no_compress").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE no_compress (id INT, data TEXT) WITH (compression_algorithm = 'none')",
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO no_compress VALUES (1, 'no compression')").expect("Failed to insert");

    let result = db.execute("SELECT * FROM no_compress").expect("Failed to select");
    assert!(result.contains("no compression"));

    env.cleanup();
}

#[test]
fn test_compression_mixed_tables() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS table_lz4").ok();
    db.execute("DROP TABLE IF EXISTS table_zstd").ok();
    db.execute("DROP TABLE IF EXISTS table_none").ok();

    db.execute("CREATE TABLE table_lz4 (id INT, data TEXT) WITH (compression_algorithm = 'lz4')")
        .expect("Failed to create lz4 table");

    db.execute("CREATE TABLE table_zstd (id INT, data TEXT) WITH (compression_algorithm = 'zstd')")
        .expect("Failed to create zstd table");

    db.execute("CREATE TABLE table_none (id INT, data TEXT) WITH (compression_algorithm = 'none')")
        .expect("Failed to create none table");

    db.execute("INSERT INTO table_lz4 VALUES (1, 'lz4 data')").expect("Failed to insert lz4");
    db.execute("INSERT INTO table_zstd VALUES (1, 'zstd data')").expect("Failed to insert zstd");
    db.execute("INSERT INTO table_none VALUES (1, 'none data')").expect("Failed to insert none");

    let result_lz4 = db.execute("SELECT * FROM table_lz4").expect("Failed to select lz4");
    assert!(result_lz4.contains("lz4 data"));

    let result_zstd = db.execute("SELECT * FROM table_zstd").expect("Failed to select zstd");
    assert!(result_zstd.contains("zstd data"));

    let result_none = db.execute("SELECT * FROM table_none").expect("Failed to select none");
    assert!(result_none.contains("none data"));

    env.cleanup();
}

#[test]
fn test_compression_large_text_column() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS large_text_compressed").expect("Failed to drop table");

    db.execute("CREATE TABLE large_text_compressed (id INT, large_content TEXT) WITH (compression_algorithm = 'lz4')")
        .expect("Failed to create table");

    let content = (0..2000).map(|i| (b'a' + (i % 26)) as char).collect::<String>();

    db.execute(&format!("INSERT INTO large_text_compressed VALUES (1, '{}')", content))
        .expect("Failed to insert large text");

    let result = db
        .execute("SELECT LENGTH(large_content) FROM large_text_compressed")
        .expect("Failed to get length");
    assert!(result.contains("2000"));

    env.cleanup();
}

#[test]
fn test_compression_binary_data() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS binary_compressed").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE binary_compressed (id INT, data BYTEA) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    let binary_data = (0..1000).map(|i| (i % 256) as u8).collect::<Vec<_>>();
    let hex_string = binary_data.iter().map(|b| format!("{:02x}", b)).collect::<String>();

    db.execute(&format!("INSERT INTO binary_compressed VALUES (1, '\\x{}')", hex_string))
        .expect("Failed to insert binary data");

    let result = db
        .execute("SELECT LENGTH(data) FROM binary_compressed")
        .expect("Failed to get binary length");
    assert!(result.contains("1000"));

    env.cleanup();
}

#[test]
fn test_compression_after_update() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS update_compressed").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE update_compressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO update_compressed VALUES (1, 'original')").expect("Failed to insert");

    db.execute("UPDATE update_compressed SET data = 'updated' WHERE id = 1")
        .expect("Failed to update");

    let result =
        db.execute("SELECT * FROM update_compressed").expect("Failed to select after update");
    assert!(result.contains("updated"));
    assert!(!result.contains("original"));

    env.cleanup();
}

#[test]
fn test_compression_after_delete() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    db.verify_connection();

    db.execute("DROP TABLE IF EXISTS delete_compressed").expect("Failed to drop table");

    db.execute(
        "CREATE TABLE delete_compressed (id INT, data TEXT) WITH (compression_algorithm = 'lz4')",
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO delete_compressed VALUES (1, 'delete me')").expect("Failed to insert");

    db.execute("DELETE FROM delete_compressed WHERE id = 1").expect("Failed to delete");

    let result =
        db.execute("SELECT COUNT(*) FROM delete_compressed").expect("Failed to count after delete");
    assert!(result.contains("0"));

    env.cleanup();
}
