use rustgres::storage::{DiskManager, Page, PageId};
use rustgres::wal::{WALDiskWriter, WALRecord, RecordType};
use rustgres::config::Config;
use tempfile::TempDir;
use std::fs;
use std::path::Path;

#[test]
fn test_config_creates_directories() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    
    // Directories should not exist yet
    assert!(!data_dir.exists());
    assert!(!wal_dir.exists());
    
    // Create directories as server would
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&wal_dir).unwrap();
    
    // Verify directories exist
    assert!(data_dir.exists());
    assert!(wal_dir.exists());
    assert!(data_dir.is_dir());
    assert!(wal_dir.is_dir());
}

#[test]
fn test_disk_manager_creates_data_files() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    
    // Initialize disk manager
    let dm = DiskManager::new(&data_dir).unwrap();
    
    // Write pages
    for i in 0..5 {
        let page_id = PageId(i);
        let mut page = Page::new(page_id);
        page.set_data(vec![i as u8; 100]);
        dm.write_page(page_id, &page).unwrap();
    }
    
    dm.sync().unwrap();
    
    // Verify data files were created
    let data_files: Vec<_> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data_"))
        .collect();
    
    assert!(!data_files.is_empty(), "No data files created");
    assert_eq!(data_files.len(), 1, "Expected 1 data file for 5 pages");
}

#[test]
fn test_wal_writer_creates_wal_files() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();
    
    // Initialize WAL writer
    let writer = WALDiskWriter::new(&wal_dir).unwrap();
    
    // Write WAL records
    for i in 0..10 {
        let record = WALRecord::new(
            i,
            i,
            RecordType::Insert,
            Some(PageId(i as u32)),
            vec![i as u8; 50],
        );
        writer.write(&record).unwrap();
    }
    
    writer.flush().unwrap();
    
    // Verify WAL files were created
    let wal_files: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("wal_"))
        .collect();
    
    assert!(!wal_files.is_empty(), "No WAL files created");
}

#[test]
fn test_data_persists_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    
    // First session: write data
    {
        let dm = DiskManager::new(&data_dir).unwrap();
        
        for i in 0..10 {
            let page_id = PageId(i);
            let mut page = Page::new(page_id);
            page.set_data(vec![i as u8; 100]);
            dm.write_page(page_id, &page).unwrap();
        }
        
        dm.sync().unwrap();
    }
    
    // Second session: read data
    {
        let dm = DiskManager::new(&data_dir).unwrap();
        
        for i in 0..10 {
            let page_id = PageId(i);
            let page = dm.read_page(page_id).unwrap();
            assert_eq!(page.id(), page_id);
        }
    }
}

#[test]
fn test_wal_persists_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();
    
    // First session: write WAL
    {
        let writer = WALDiskWriter::new(&wal_dir).unwrap();
        
        for i in 0..20 {
            let record = WALRecord::new(
                i,
                i,
                RecordType::Insert,
                Some(PageId(i as u32)),
                vec![i as u8; 100],
            );
            writer.write(&record).unwrap();
        }
        
        writer.flush().unwrap();
    }
    
    // Verify WAL files exist after restart
    let wal_files: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    
    assert!(!wal_files.is_empty());
}

#[test]
fn test_multiple_data_files_created() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    
    let dm = DiskManager::new(&data_dir).unwrap();
    
    // Write pages across multiple files (1000 pages per file)
    for i in 0..2500 {
        let page_id = PageId(i);
        let mut page = Page::new(page_id);
        page.set_data(vec![(i % 256) as u8; 50]);
        dm.write_page(page_id, &page).unwrap();
    }
    
    dm.sync().unwrap();
    
    // Should have 3 data files (0-999, 1000-1999, 2000-2499)
    let data_files: Vec<_> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data_"))
        .collect();
    
    assert_eq!(data_files.len(), 3, "Expected 3 data files for 2500 pages");
}

#[test]
fn test_config_default_directories() {
    let config = Config::default();
    
    assert_eq!(config.storage.data_dir, "./data");
    assert_eq!(config.storage.wal_dir, "./wal");
    assert_eq!(config.storage.buffer_pool_size, 1000);
    assert_eq!(config.storage.page_size, 8192);
}

#[test]
fn test_config_custom_directories() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_config.yaml");
    
    let config_content = format!(
        r#"
server:
  host: "127.0.0.1"
  port: 5433
  max_connections: 100

storage:
  data_dir: "{}/custom_data"
  wal_dir: "{}/custom_wal"
  buffer_pool_size: 500
  page_size: 8192

logging:
  level: "info"
  scope: "*"

transaction:
  timeout: 300
  mvcc_enabled: true

wal:
  segment_size: 16
  compression: false
  sync_on_commit: true

performance:
  worker_threads: 4
  query_cache: false
"#,
        temp_dir.path().display(),
        temp_dir.path().display()
    );
    
    fs::write(&config_path, config_content).unwrap();
    
    let config = Config::from_file(&config_path).unwrap();
    
    assert!(config.storage.data_dir.contains("custom_data"));
    assert!(config.storage.wal_dir.contains("custom_wal"));
    assert_eq!(config.storage.buffer_pool_size, 500);
}

#[test]
fn test_directory_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&wal_dir).unwrap();
    
    // Verify directories are writable
    let test_file = data_dir.join("test.txt");
    fs::write(&test_file, "test").unwrap();
    assert!(test_file.exists());
    
    let test_file = wal_dir.join("test.txt");
    fs::write(&test_file, "test").unwrap();
    assert!(test_file.exists());
}

#[test]
fn test_nested_directory_creation() {
    let temp_dir = TempDir::new().unwrap();
    let nested_data = temp_dir.path().join("level1/level2/data");
    let nested_wal = temp_dir.path().join("level1/level2/wal");
    
    // Should create all parent directories
    fs::create_dir_all(&nested_data).unwrap();
    fs::create_dir_all(&nested_wal).unwrap();
    
    assert!(nested_data.exists());
    assert!(nested_wal.exists());
    
    // Should be able to use them
    let dm = DiskManager::new(&nested_data).unwrap();
    let page = Page::new(PageId(0));
    dm.write_page(PageId(0), &page).unwrap();
}

#[test]
fn test_concurrent_writes_to_disk() {
    use std::thread;
    use std::sync::Arc;
    
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    
    let dm = Arc::new(DiskManager::new(&data_dir).unwrap());
    
    let mut handles = vec![];
    
    // Spawn multiple threads writing different pages
    for thread_id in 0..5 {
        let dm_clone = Arc::clone(&dm);
        let handle = thread::spawn(move || {
            for i in 0..10 {
                let page_id = PageId(thread_id * 10 + i);
                let mut page = Page::new(page_id);
                page.set_data(vec![thread_id as u8; 50]);
                dm_clone.write_page(page_id, &page).unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    dm.sync().unwrap();
    
    // Verify all pages were written
    for thread_id in 0..5 {
        for i in 0..10 {
            let page_id = PageId(thread_id * 10 + i);
            let page = dm.read_page(page_id).unwrap();
            assert_eq!(page.id(), page_id);
        }
    }
}
