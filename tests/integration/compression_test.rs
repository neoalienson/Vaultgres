use std::sync::Arc;
use vaultgres::storage::btree::{BTree, TupleId};
use vaultgres::storage::compression::{
    compress, decompress, should_compress, CompressionAlgorithm, CompressionContext,
};
use vaultgres::storage::heap::HeapFile;
use vaultgres::storage::index::brin::BRINIndex;
use vaultgres::storage::index::gin::GINIndex;
use vaultgres::storage::index::gist::GiSTIndex;
use vaultgres::storage::index::hash::HashIndex;
use vaultgres::storage::{BufferPool, PageId};

#[test]
fn test_compression_lz4() {
    let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();

    let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    assert!(compressed.len() < data.len());

    let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4, data.len()).unwrap();
    assert_eq!(data, decompressed);
}

#[test]
fn test_compression_zstd() {
    let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();

    let compressed = compress(&data, CompressionAlgorithm::Zstd).unwrap();
    assert!(compressed.len() < data.len());

    let decompressed = decompress(&compressed, CompressionAlgorithm::Zstd, data.len()).unwrap();
    assert_eq!(data, decompressed);
}

#[test]
fn test_compression_threshold() {
    let small_data = vec![0u8; 100];
    assert!(!should_compress(small_data.len()));

    let large_data = vec![0u8; 3000];
    assert!(should_compress(large_data.len()));
}

#[test]
fn test_no_compression_small() {
    let data = vec![0u8; 100];

    let result = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_mixed_compressed_uncompressed() {
    let pool = Arc::new(BufferPool::new(10));
    let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

    let small_data = vec![1u8; 100];
    let large_data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();

    heap.insert_compressed(&small_data).unwrap();
    heap.insert_compressed(&large_data).unwrap();

    assert_eq!(heap.tuple_count(), 2);
}

#[test]
fn test_compression_context() {
    let mut ctx = CompressionContext::new(CompressionAlgorithm::Lz4);

    let data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();

    let compressed = ctx.compress(&data).unwrap();
    let decompressed = ctx.decompress(&compressed, data.len()).unwrap();

    assert_eq!(data, decompressed);
    assert!(ctx.stats().compression_count > 0);
}

#[test]
fn test_buffer_pool_compressed_pages() {
    let pool = BufferPool::with_compression(10, CompressionAlgorithm::Lz4);

    pool.fetch(PageId(1)).unwrap();
    pool.unpin_and_compress(PageId(1), CompressionAlgorithm::Lz4).unwrap();

    assert!(pool.is_page_compressed(PageId(1)));
}

#[test]
fn test_heap_compression_allocation() {
    let pool = Arc::new(BufferPool::new(10));
    let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

    for i in 0..5 {
        let data: Vec<u8> = (0..3000).map(|_| i as u8).collect();
        heap.insert_compressed(&data).unwrap();
    }

    assert_eq!(heap.tuple_count(), 5);
}

#[test]
fn test_btree_compression() {
    let mut tree = BTree::with_compression(128, CompressionAlgorithm::Lz4);

    for i in 0..10 {
        let key: Vec<u8> = (0..200).map(|j| ((i * 200 + j) % 256) as u8).collect();
        let value = TupleId { page_id: PageId(i as u32), slot: 0 };
        tree.insert_compressed(&key, value).unwrap();
    }

    assert_eq!(tree.key_count(), 10);
}

#[test]
fn test_btree_compress_decompress_node() {
    let mut tree = BTree::with_compression(128, CompressionAlgorithm::Lz4);

    for i in 0..20 {
        let key: Vec<u8> = vec![i as u8; 100];
        let value = TupleId { page_id: PageId(i), slot: 0 };
        tree.insert(key, value).unwrap();
    }

    let initial_count = tree.key_count();

    tree.compress_node().unwrap();

    if tree.is_compressed() {
        tree.decompress_node().unwrap();
        assert!(!tree.is_compressed());
    }

    assert_eq!(tree.key_count(), initial_count);
}

#[test]
fn test_hash_index_compression() {
    let mut index = HashIndex::with_compression(16, CompressionAlgorithm::Lz4);

    for i in 0..10 {
        let key: Vec<u8> = (0..200).map(|j| ((i * 200 + j) % 256) as u8).collect();
        let tid = (PageId(i as u32), 0);
        index.insert_compressed(&key, tid).unwrap();
    }

    assert!(index.search(&vec![0u8; 200]).is_ok());
}

#[test]
fn test_hash_index_compress_bucket() {
    let mut index = HashIndex::with_compression(4, CompressionAlgorithm::Lz4);

    for i in 0..8 {
        let key = vec![i as u8; 100];
        let tid = (PageId(i as u32), 0);
        index.insert(&key, tid).unwrap();
    }

    index.compress_bucket(0).unwrap();

    if index.is_bucket_compressed(0) {
        index.decompress_bucket(0).unwrap();
        assert!(!index.is_bucket_compressed(0));
    }
}

#[test]
fn test_brin_index_compression() {
    let mut index = BRINIndex::with_compression(128, CompressionAlgorithm::Lz4);

    for i in 0..20 {
        let key: Vec<u8> = (0..200).map(|j| ((i * 200 + j) % 256) as u8).collect();
        let tid = (PageId(i * 128), 0);
        index.insert_compressed(&key, tid).unwrap();
    }

    assert!(index.range_count() > 0);
}

#[test]
fn test_brin_index_compress_range() {
    let mut index = BRINIndex::with_compression(128, CompressionAlgorithm::Lz4);

    for i in 0..10 {
        let key: Vec<u8> = vec![i as u8; 100];
        let tid = (PageId(i * 128), 0);
        index.insert(&key, tid).unwrap();
    }

    index.compress_range(0).unwrap();

    if index.is_range_compressed(0) {
        index.decompress_range(0).unwrap();
        assert!(!index.is_range_compressed(0));
    }
}

#[test]
fn test_gin_index_compression() {
    let mut index = GINIndex::with_compression(CompressionAlgorithm::Lz4);

    for i in 0..10 {
        let key: Vec<u8> = (0..200).map(|j| ((i * 200 + j) % 256) as u8).collect();
        let tid = (PageId(i as u32), 0);
        index.insert_compressed(&key, tid).unwrap();
    }

    let keys = index.keys();
    assert!(keys.len() > 0);
}

#[test]
fn test_gist_index_compression() {
    let mut index = GiSTIndex::with_compression(CompressionAlgorithm::Lz4);

    for i in 0..10 {
        let key: Vec<u8> = (0..200).map(|j| ((i * 200 + j) % 256) as u8).collect();
        let tid = (PageId(i as u32), 0);
        index.insert_compressed(&key, tid).unwrap();
    }

    assert!(!index.is_compressed());
}

#[test]
fn test_pattern_data_compression_ratio() {
    let data: Vec<u8> = vec![0u8; 10000];

    let compressed_lz4 = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let ratio_lz4 = compressed_lz4.len() as f64 / data.len() as f64;
    assert!(ratio_lz4 < 0.1, "Pattern data should compress very well with LZ4");

    let compressed_zstd = compress(&data, CompressionAlgorithm::Zstd).unwrap();
    let ratio_zstd = compressed_zstd.len() as f64 / data.len() as f64;
    assert!(ratio_zstd < 0.1, "Pattern data should compress very well with Zstd");
}

#[test]
fn test_random_data_compression() {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let data: Vec<u8> = (0..10000).map(|_| rng.gen()).collect();

    let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let ratio = compressed.len() as f64 / data.len() as f64;
    assert!(ratio > 0.5, "Random data should not compress well");
}

#[test]
fn test_empty_data_compression() {
    let data: Vec<u8> = vec![];

    let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4, 0).unwrap();
    assert_eq!(data, decompressed);
}

#[test]
fn test_large_data_compression() {
    let data: Vec<u8> = (0..100000).map(|i| (i % 256) as u8).collect();

    let compressed_lz4 = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let decompressed_lz4 =
        decompress(&compressed_lz4, CompressionAlgorithm::Lz4, data.len()).unwrap();
    assert_eq!(data, decompressed_lz4);

    let compressed_zstd = compress(&data, CompressionAlgorithm::Zstd).unwrap();
    let decompressed_zstd =
        decompress(&compressed_zstd, CompressionAlgorithm::Zstd, data.len()).unwrap();
    assert_eq!(data, decompressed_zstd);
}

#[test]
fn test_compression_stats_tracking() {
    let mut ctx = CompressionContext::new(CompressionAlgorithm::Lz4);

    for _ in 0..5 {
        let data: Vec<u8> = vec![0u8; 3000];
        ctx.compress(&data).unwrap();
    }

    assert_eq!(ctx.stats().compression_count, 5);
    assert_eq!(ctx.stats().bytes_uncompressed, 5 * 3000);
    assert!(ctx.stats().bytes_compressed > 0);
}

#[test]
fn test_alter_table_compression() {
    use vaultgres::catalog::schema::{TableSchema, TableStorageOptions};
    use vaultgres::parser::ast::ColumnDef;

    let columns = vec![
        ColumnDef::new("id".to_string(), vaultgres::parser::ast::DataType::Int),
        ColumnDef::new("data".to_string(), vaultgres::parser::ast::DataType::Text),
    ];

    let opts = TableStorageOptions::new(CompressionAlgorithm::Lz4);
    let schema = TableSchema::with_storage_options("test_table".to_string(), columns, opts);

    assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::Lz4);

    let mut schema = schema;
    schema.set_compression_algorithm(CompressionAlgorithm::Zstd);
    assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::Zstd);
}

#[test]
fn test_compressed_heap_with_schema() {
    use vaultgres::catalog::schema::{TableSchema, TableStorageOptions};
    use vaultgres::parser::ast::ColumnDef;

    let columns = vec![
        ColumnDef::new("id".to_string(), vaultgres::parser::ast::DataType::Int),
        ColumnDef::new("large_text".to_string(), vaultgres::parser::ast::DataType::Text),
    ];

    let opts = TableStorageOptions::new(CompressionAlgorithm::Zstd);
    let schema = TableSchema::with_storage_options("compressed_table".to_string(), columns, opts);

    let pool = Arc::new(BufferPool::new(10));
    let heap = HeapFile::with_schema(pool, &schema);

    assert_eq!(heap.compression_algorithm(), CompressionAlgorithm::Zstd);
}

#[test]
fn test_compression_algorithm_from_str() {
    assert_eq!(CompressionAlgorithm::from_str("lz4"), Some(CompressionAlgorithm::Lz4));
    assert_eq!(CompressionAlgorithm::from_str("LZ4"), Some(CompressionAlgorithm::Lz4));
    assert_eq!(CompressionAlgorithm::from_str("zstd"), Some(CompressionAlgorithm::Zstd));
    assert_eq!(CompressionAlgorithm::from_str("none"), Some(CompressionAlgorithm::None));
    assert_eq!(CompressionAlgorithm::from_str("off"), Some(CompressionAlgorithm::None));
    assert_eq!(CompressionAlgorithm::from_str("invalid"), None);
}

#[test]
fn test_algorithm_display() {
    assert_eq!(CompressionAlgorithm::Lz4.to_string(), "lz4");
    assert_eq!(CompressionAlgorithm::Zstd.to_string(), "zstd");
    assert_eq!(CompressionAlgorithm::None.to_string(), "none");
}

#[test]
fn test_edge_case_single_byte() {
    let data = vec![42u8];
    let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4, data.len()).unwrap();
    assert_eq!(data, decompressed);
}

#[test]
fn test_edge_case_all_same_bytes() {
    let data = vec![255u8; 10000];
    let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4, data.len()).unwrap();
    assert_eq!(data, decompressed);
    assert!(compressed.len() < 100);
}

#[test]
fn test_edge_case_alternating_bytes() {
    let data: Vec<u8> = (0..10000).map(|i| if i % 2 == 0 { 0 } else { 255 }).collect();
    let compressed = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4, data.len()).unwrap();
    assert_eq!(data, decompressed);
}

#[test]
fn test_buffer_pool_fetch_decompressed() {
    let pool = BufferPool::with_compression(10, CompressionAlgorithm::Lz4);

    pool.fetch(PageId(1)).unwrap();
    pool.unpin_and_compress(PageId(1), CompressionAlgorithm::Lz4).unwrap();
    assert!(pool.is_page_compressed(PageId(1)));

    pool.fetch_decompressed(PageId(1), CompressionAlgorithm::Lz4).unwrap();
    assert!(!pool.is_page_compressed(PageId(1)));
}

#[test]
fn test_multiple_algorithms_same_data() {
    let data: Vec<u8> = vec![0u8; 5000];

    let compressed_lz4 = compress(&data, CompressionAlgorithm::Lz4).unwrap();
    let compressed_zstd = compress(&data, CompressionAlgorithm::Zstd).unwrap();

    assert!(compressed_lz4.len() < data.len());
    assert!(compressed_zstd.len() < data.len());

    let decompressed_lz4 =
        decompress(&compressed_lz4, CompressionAlgorithm::Lz4, data.len()).unwrap();
    let decompressed_zstd =
        decompress(&compressed_zstd, CompressionAlgorithm::Zstd, data.len()).unwrap();

    assert_eq!(decompressed_lz4, data);
    assert_eq!(decompressed_zstd, data);
}
