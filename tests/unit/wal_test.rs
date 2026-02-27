use rustgres::wal::{WALWriter, WALRecord, RecordType};
use rustgres::storage::PageId;

#[test]
fn test_wal_writer_creation() {
    let writer = WALWriter::new();
    assert_eq!(writer.current_lsn(), 0);
    assert_eq!(writer.flushed_lsn(), 0);
}

#[test]
fn test_write_single_record() {
    let writer = WALWriter::new();
    let lsn = writer.write(1, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3]).unwrap();
    
    assert_eq!(lsn, 1);
    assert_eq!(writer.current_lsn(), 1);
}

#[test]
fn test_write_multiple_records() {
    let writer = WALWriter::new();
    
    let lsn1 = writer.write(1, RecordType::Insert, Some(PageId(1)), vec![]).unwrap();
    let lsn2 = writer.write(1, RecordType::Update, Some(PageId(2)), vec![]).unwrap();
    let lsn3 = writer.write(1, RecordType::Delete, Some(PageId(3)), vec![]).unwrap();
    
    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    assert_eq!(lsn3, 3);
}

#[test]
fn test_lsn_monotonic_increase() {
    let writer = WALWriter::new();
    let mut prev_lsn = 0;
    
    for _ in 0..100 {
        let lsn = writer.write(1, RecordType::Insert, None, vec![]).unwrap();
        assert!(lsn > prev_lsn);
        prev_lsn = lsn;
    }
}

#[test]
fn test_flush_empty_buffer() {
    let writer = WALWriter::new();
    let lsn = writer.flush().unwrap();
    
    assert_eq!(lsn, 0);
}

#[test]
fn test_flush_updates_flushed_lsn() {
    let writer = WALWriter::new();
    
    writer.write(1, RecordType::Insert, None, vec![]).unwrap();
    writer.write(1, RecordType::Update, None, vec![]).unwrap();
    
    let flushed = writer.flush().unwrap();
    assert_eq!(flushed, 2);
    assert_eq!(writer.flushed_lsn(), 2);
}

#[test]
fn test_flush_clears_buffer() {
    let writer = WALWriter::new();
    
    writer.write(1, RecordType::Insert, None, vec![]).unwrap();
    writer.flush().unwrap();
    
    let records = writer.get_records();
    assert_eq!(records.len(), 0);
}

#[test]
fn test_record_types() {
    let writer = WALWriter::new();
    
    writer.write(1, RecordType::Insert, None, vec![]).unwrap();
    writer.write(1, RecordType::Update, None, vec![]).unwrap();
    writer.write(1, RecordType::Delete, None, vec![]).unwrap();
    writer.write(1, RecordType::Commit, None, vec![]).unwrap();
    writer.write(1, RecordType::Abort, None, vec![]).unwrap();
    writer.write(1, RecordType::Checkpoint, None, vec![]).unwrap();
    
    assert_eq!(writer.current_lsn(), 6);
}

#[test]
fn test_record_with_page_id() {
    let writer = WALWriter::new();
    let lsn = writer.write(1, RecordType::Insert, Some(PageId(42)), vec![]).unwrap();
    
    assert_eq!(lsn, 1);
}

#[test]
fn test_record_without_page_id() {
    let writer = WALWriter::new();
    let lsn = writer.write(1, RecordType::Commit, None, vec![]).unwrap();
    
    assert_eq!(lsn, 1);
}

#[test]
fn test_record_with_data() {
    let writer = WALWriter::new();
    let data = vec![1, 2, 3, 4, 5];
    let lsn = writer.write(1, RecordType::Insert, None, data).unwrap();
    
    assert_eq!(lsn, 1);
}

#[test]
fn test_record_with_empty_data() {
    let writer = WALWriter::new();
    let lsn = writer.write(1, RecordType::Commit, None, vec![]).unwrap();
    
    assert_eq!(lsn, 1);
}

#[test]
fn test_record_with_large_data() {
    let writer = WALWriter::new();
    let data = vec![0u8; 10000];
    let lsn = writer.write(1, RecordType::Insert, None, data).unwrap();
    
    assert_eq!(lsn, 1);
}

#[test]
fn test_multiple_transactions() {
    let writer = WALWriter::new();
    
    writer.write(1, RecordType::Insert, None, vec![]).unwrap();
    writer.write(2, RecordType::Insert, None, vec![]).unwrap();
    writer.write(3, RecordType::Insert, None, vec![]).unwrap();
    
    assert_eq!(writer.current_lsn(), 3);
}

#[test]
fn test_wal_record_serialization() {
    let record = WALRecord::new(1, 10, RecordType::Insert, Some(PageId(5)), vec![1, 2, 3]);
    let bytes = record.to_bytes();
    
    assert!(!bytes.is_empty());
    assert!(bytes.len() > 3); // At least header + data
}

#[test]
fn test_wal_record_fields() {
    let record = WALRecord::new(42, 100, RecordType::Update, Some(PageId(7)), vec![1, 2]);
    
    assert_eq!(record.lsn, 42);
    assert_eq!(record.xid, 100);
    assert_eq!(record.record_type, RecordType::Update);
    assert_eq!(record.page_id, Some(PageId(7)));
    assert_eq!(record.data, vec![1, 2]);
}
