//! Standalone demonstration of view expansion
//! Run with: cargo run --example view_demo

use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr, SelectStmt};

fn main() {
    println!("=== View Expansion Demonstration ===\n");

    // Create catalog
    let catalog = Catalog::new();

    // Create users table
    println!("1. Creating users table...");
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
        ColumnDef::new("age".to_string(), DataType::Int),
    ];
    catalog.create_table("users".to_string(), columns).unwrap();

    // Insert test data
    println!("2. Inserting test data...");
    catalog
        .insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string()), Expr::Number(25)])
        .unwrap();

    catalog
        .insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string()), Expr::Number(30)])
        .unwrap();

    catalog
        .insert(
            "users",
            vec![Expr::Number(3), Expr::String("Charlie".to_string()), Expr::Number(35)],
        )
        .unwrap();

    catalog
        .insert("users", vec![Expr::Number(4), Expr::String("David".to_string()), Expr::Number(28)])
        .unwrap();

    println!("   Inserted 4 users: Alice(25), Bob(30), Charlie(35), David(28)");

    // Create view: young_users (age < 30)
    println!("\n3. Creating view 'young_users' (age < 30)...");
    let view_query = SelectStmt {
        distinct: false,
        columns: vec![
            Expr::Column("id".to_string()),
            Expr::Column("name".to_string()),
            Expr::Column("age".to_string()),
        ],
        from: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(30)),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    catalog.create_view("young_users".to_string(), view_query).unwrap();
    println!("   View created successfully!");

    // Query the view directly
    println!("\n4. Querying view: SELECT id, name, age FROM young_users");
    let results = catalog
        .select(
            "young_users",
            false,
            vec![
                Expr::Column("id".to_string()),
                Expr::Column("name".to_string()),
                Expr::Column("age".to_string()),
            ],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

    println!("   Results ({} rows):", results.len());
    for (i, row) in results.iter().enumerate() {
        println!("     Row {}: {:?}", i + 1, row);
    }

    // Query view with outer filter
    println!("\n5. Querying view with outer filter: SELECT name FROM young_users WHERE id > 1");
    let results = catalog
        .select(
            "young_users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(1)),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

    println!("   Results ({} rows):", results.len());
    for (i, row) in results.iter().enumerate() {
        println!("     Row {}: {:?}", i + 1, row);
    }

    // Verify view expansion worked correctly
    println!("\n6. Verification:");
    println!("   - View 'young_users' filters age < 30 (Alice=25, David=28)");
    println!("   - Outer filter id > 1 excludes Alice (id=1)");
    println!("   - Expected result: David only");
    println!("   - Actual result: {} row(s)", results.len());

    if results.len() == 1 {
        println!("\n✓ View expansion working correctly!");
    } else {
        println!("\n✗ Unexpected result count");
    }

    println!("\n=== Demonstration Complete ===");
}
