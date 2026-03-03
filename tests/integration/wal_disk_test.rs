use std::sync::Arc;
use tempfile::TempDir;
use vaultgres::storage::PageId;
use vaultgres::wal::{RecordType, WALDiskWriter, WALWriter};

#[test]
fn test_wal_writer_with_disk() {
    let temp_dir = TempDir::new().unwrap();
    let dw = Arc::new(WALDiskWriter::new(temp_dir.path()).unwrap());
    let writer = WALWriter::with_disk(dw);

    let lsn = writer.write(1, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3]).unwrap();
    assert_eq!(lsn, 1);

    writer.flush().unwrap();
}

#[test]
fn test_wal_flush_writes_to_disk() {
    let temp_dir = TempDir::new().unwrap();
    let dw = Arc::new(WALDiskWriter::new(temp_dir.path()).unwrap());
    let writer = WALWriter::with_disk(dw);

    // Write multiple records
    for i in 0..10 {
        writer.write(i, RecordType::Insert, Some(PageId(i as u32)), vec![]).unwrap();
    }

    let flushed_lsn = writer.flush().unwrap();
    assert_eq!(flushed_lsn, 10);
}

#[test]
fn test_wal_multiple_flushes() {
    let temp_dir = TempDir::new().unwrap();
    let dw = Arc::new(WALDiskWriter::new(temp_dir.path()).unwrap());
    let writer = WALWriter::with_disk(dw);

    writer.write(1, RecordType::Insert, Some(PageId(1)), vec![]).unwrap();
    writer.flush().unwrap();

    writer.write(2, RecordType::Update, Some(PageId(2)), vec![]).unwrap();
    writer.flush().unwrap();

    assert_eq!(writer.flushed_lsn(), 2);
}

#[test]
fn test_wal_all_record_types() {
    let temp_dir = TempDir::new().unwrap();
    let dw = Arc::new(WALDiskWriter::new(temp_dir.path()).unwrap());
    let writer = WALWriter::with_disk(dw);

    let types = [
        RecordType::Insert,
        RecordType::Update,
        RecordType::Delete,
        RecordType::Commit,
        RecordType::Abort,
        RecordType::Checkpoint,
    ];

    for (i, record_type) in types.iter().enumerate() {
        writer.write((i + 1) as u64, *record_type, Some(PageId(i as u32)), vec![]).unwrap();
    }

    writer.flush().unwrap();
}
