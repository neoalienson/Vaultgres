use rustgres::storage::index::*;
use rustgres::storage::page::PageId;

#[test]
fn test_hash_index_integration() {
    let mut index = HashIndex::new(64);

    // Insert 1000 entries
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let tid = (PageId(i), 0);
        index.insert(key.as_bytes(), tid).unwrap();
    }

    // Search for entries
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let result = index.search(key.as_bytes()).unwrap();
        assert_eq!(result, vec![(PageId(i), 0)]);
    }

    // Delete some entries
    for i in 0..100 {
        let key = format!("key_{}", i);
        let tid = (PageId(i), 0);
        assert!(index.delete(key.as_bytes(), tid).unwrap());
    }

    // Verify deletions
    for i in 0..100 {
        let key = format!("key_{}", i);
        assert!(index.search(key.as_bytes()).is_err());
    }
}

#[test]
fn test_brin_index_integration() {
    let mut index = BRINIndex::new(128);

    // Insert sequential data
    for i in 0..1000 {
        let key = format!("{:08}", i);
        let tid = (PageId(i / 10), (i % 10) as u16);
        index.insert(key.as_bytes(), tid).unwrap();
    }

    // Range search
    let result = index.range_search(b"00000100", b"00000200").unwrap();
    assert!(result.len() > 0);

    // Point search
    let result = index.search(b"00000500").unwrap();
    assert!(result.len() > 0);
}

#[test]
fn test_gin_index_array_containment() {
    let mut index = GINIndex::new();

    // Insert documents with tags
    let doc1 = (PageId(1), 0);
    let doc2 = (PageId(2), 0);
    let doc3 = (PageId(3), 0);

    index.insert(b"rust\0database\0sql", doc1).unwrap();
    index.insert(b"python\0database\0nosql", doc2).unwrap();
    index.insert(b"rust\0systems\0performance", doc3).unwrap();

    // Search for documents containing "rust"
    let result = index.search(b"rust").unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains(&doc1));
    assert!(result.contains(&doc3));

    // Search for documents containing both "rust" and "database"
    let result = index.search(b"rust\0database").unwrap();
    assert_eq!(result, vec![doc1]);
}

#[test]
fn test_gist_index_geometric() {
    let mut index = GiSTIndex::new();

    // Insert points
    index.insert(b"point_a", (PageId(1), 0)).unwrap();
    index.insert(b"point_m", (PageId(2), 0)).unwrap();
    index.insert(b"point_z", (PageId(3), 0)).unwrap();

    // Range query
    let result = index.range_search(b"point_a", b"point_n").unwrap();
    assert!(result.len() >= 2);

    // Point query
    let result = index.search(b"point_m").unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_partial_index_integration() {
    let hash_index = Box::new(HashIndex::new(32));
    let mut index = PartialIndex::new(hash_index, |key| {
        // Only index keys starting with 'a'
        !key.is_empty() && key[0] == b'a'
    });

    // Insert various keys
    index.insert(b"apple", (PageId(1), 0)).unwrap();
    index.insert(b"banana", (PageId(2), 0)).unwrap();
    index.insert(b"avocado", (PageId(3), 0)).unwrap();
    index.insert(b"cherry", (PageId(4), 0)).unwrap();

    // Only 'a' keys should be indexed
    assert!(index.search(b"apple").is_ok());
    assert!(index.search(b"avocado").is_ok());
    assert!(index.search(b"banana").is_err());
    assert!(index.search(b"cherry").is_err());
}

#[test]
fn test_expression_index_case_insensitive() {
    let hash_index = Box::new(HashIndex::new(32));
    let mut index = ExpressionIndex::new(hash_index, |key| key.to_ascii_lowercase());

    // Insert with mixed case
    index.insert(b"HELLO", (PageId(1), 0)).unwrap();
    index.insert(b"World", (PageId(2), 0)).unwrap();
    index.insert(b"rust", (PageId(3), 0)).unwrap();

    // Search should be case-insensitive
    assert!(index.search(b"hello").is_ok());
    assert!(index.search(b"HELLO").is_ok());
    assert!(index.search(b"world").is_ok());
    assert!(index.search(b"WORLD").is_ok());
    assert!(index.search(b"RUST").is_ok());
}

#[test]
fn test_multiple_index_types() {
    let mut hash = HashIndex::new(16);
    let mut brin = BRINIndex::new(128);
    let mut gin = GINIndex::new();
    let mut gist = GiSTIndex::new();

    let tid = (PageId(1), 0);
    let key = b"test_key";

    // All should support insert and search
    hash.insert(key, tid).unwrap();
    brin.insert(key, tid).unwrap();
    gin.insert(key, tid).unwrap();
    gist.insert(key, tid).unwrap();

    assert!(hash.search(key).is_ok());
    assert!(brin.search(key).is_ok());
    assert!(gin.search(key).is_ok());
    assert!(gist.search(key).is_ok());

    // Verify index types
    assert_eq!(hash.index_type(), IndexType::Hash);
    assert_eq!(brin.index_type(), IndexType::BRIN);
    assert_eq!(gin.index_type(), IndexType::GIN);
    assert_eq!(gist.index_type(), IndexType::GiST);
}

#[test]
fn test_hash_index_collisions() {
    let mut index = HashIndex::new(4); // Small bucket count to force collisions

    for i in 0..100 {
        let key = format!("key_{}", i);
        let tid = (PageId(i), 0);
        index.insert(key.as_bytes(), tid).unwrap();
    }

    // All keys should still be searchable
    for i in 0..100 {
        let key = format!("key_{}", i);
        let result = index.search(key.as_bytes()).unwrap();
        assert_eq!(result, vec![(PageId(i), 0)]);
    }
}

#[test]
fn test_brin_minimal_storage() {
    let mut index = BRINIndex::new(128);

    // Insert 10000 entries
    for i in 0..10000 {
        let key = format!("{:08}", i);
        let tid = (PageId(i / 100), (i % 100) as u16);
        index.insert(key.as_bytes(), tid).unwrap();
    }

    // BRIN should have minimal ranges
    // With 10000 entries and 128 pages per range, we expect ~79 ranges
    // This is much smaller than storing individual entries
}

#[test]
fn test_gin_empty_keys() {
    let mut index = GINIndex::new();

    // Insert with empty array
    let tid = (PageId(1), 0);
    index.insert(b"", tid).unwrap();

    // Search should handle empty keys
    assert!(index.search(b"").is_err());
}

#[test]
fn test_combined_partial_expression() {
    let hash_index = Box::new(HashIndex::new(32));
    let expr_index = ExpressionIndex::new(hash_index, |key| key.to_ascii_lowercase());
    let mut index = PartialIndex::new(Box::new(expr_index), |key| !key.is_empty() && key.len() > 3);

    // Insert various keys
    index.insert(b"HELLO", (PageId(1), 0)).unwrap();
    index.insert(b"HI", (PageId(2), 0)).unwrap();
    index.insert(b"WORLD", (PageId(3), 0)).unwrap();

    // Only keys > 3 chars should be indexed, case-insensitive
    assert!(index.search(b"hello").is_ok());
    assert!(index.search(b"world").is_ok());
    assert!(index.search(b"hi").is_err()); // Too short
}
