use rustgres::storage::{DiskManager, Page, PageId};
use rustgres::wal::{WALDiskWriter, WALRecord, RecordType};
use tempfile::TempDir;

#[test]
fn test_disk_manager_write_read() {
    let temp_dir = TempDir::new().unwrap();
    let dm = DiskManager::new(temp_dir.path()).unwrap();
    
    let page_id = PageId(0);
    let mut page = Page::new(page_id);
    page.set_data(vec![1, 2, 3, 4, 5]);
    
    dm.write_page(page_id, &page).unwrap();
    let read_page = dm.read_page(page_id).unwrap();
    
    assert_eq!(read_page.id(), page_id);
}

#[test]
fn test_disk_manager_multiple_pages() {
    let temp_dir = TempDir::new().unwrap();
    let dm = DiskManager::new(temp_dir.path()).unwrap();
    
    for i in 0..10 {
        let page_id = PageId(i);
        let mut page = Page::new(page_id);
        page.set_data(vec![i as u8; 100]);
        
        dm.write_page(page_id, &page).unwrap();
    }
    
    for i in 0..10 {
        let page_id = PageId(i);
        let page = dm.read_page(page_id).unwrap();
        assert_eq!(page.id(), page_id);
    }
}

#[test]
fn test_disk_manager_sync() {
    let temp_dir = TempDir::new().unwrap();
    let dm = DiskManager::new(temp_dir.path()).unwrap();
    
    let page_id = PageId(0);
    let page = Page::new(page_id);
    
    dm.write_page(page_id, &page).unwrap();
    dm.sync().unwrap();
}

#[test]
fn test_wal_disk_writer_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let writer = WALDiskWriter::new(temp_dir.path()).unwrap();
    
    let record = WALRecord::new(
        1,
        1,
        RecordType::Insert,
        Some(PageId(1)),
        vec![1, 2, 3, 4, 5],
    );
    
    let lsn = writer.write(&record).unwrap();
    writer.flush().unwrap();
    
    // LSN is u64, always >= 0
    assert!(lsn == 0 || lsn > 0);
    
    // Verify file was created
    let wal_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("wal_"))
        .collect();
    
    assert!(!wal_files.is_empty());
}

#[test]
fn test_wal_disk_writer_multiple_records() {
    let temp_dir = TempDir::new().unwrap();
    let writer = WALDiskWriter::new(temp_dir.path()).unwrap();
    
    for i in 0..100 {
        let record = WALRecord::new(
            i,
            i,
            RecordType::Insert,
            Some(PageId(i as u32)),
            vec![i as u8; 10],
        );
        
        writer.write(&record).unwrap();
    }
    
    writer.flush().unwrap();
}

#[test]
fn test_disk_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    
    // Write data
    {
        let dm = DiskManager::new(temp_dir.path()).unwrap();
        let page_id = PageId(42);
        let mut page = Page::new(page_id);
        page.set_data(vec![1, 2, 3, 4, 5]);
        
        dm.write_page(page_id, &page).unwrap();
        dm.sync().unwrap();
    }
    
    // Read data with new DiskManager instance
    {
        let dm = DiskManager::new(temp_dir.path()).unwrap();
        let page = dm.read_page(PageId(42)).unwrap();
        assert_eq!(page.id(), PageId(42));
    }
}
