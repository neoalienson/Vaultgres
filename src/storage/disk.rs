use super::error::Result;
use super::page::{Page, PageId};
use std::collections::HashMap;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const PAGE_SIZE: usize = 8192;

pub struct DiskManager {
    data_dir: PathBuf,
    file_handles: Mutex<HashMap<String, File>>,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        create_dir_all(&data_dir)?;

        Ok(Self { data_dir, file_handles: Mutex::new(HashMap::new()) })
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let file_path = self.page_file_path(page_id);
        let mut handles = self.file_handles.lock().unwrap();

        let file = handles.entry(file_path.to_string_lossy().to_string()).or_insert_with(|| {
            OpenOptions::new().read(true).write(true).create(true).open(&file_path).unwrap()
        });

        let offset = self.page_offset(page_id);
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer)?;

        Ok(Page::from_bytes(&buffer))
    }

    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        let file_path = self.page_file_path(page_id);
        let mut handles = self.file_handles.lock().unwrap();

        let file = handles.entry(file_path.to_string_lossy().to_string()).or_insert_with(|| {
            OpenOptions::new().read(true).write(true).create(true).open(&file_path).unwrap()
        });

        let offset = self.page_offset(page_id);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page.to_bytes())?;

        log::trace!("Wrote page {} to disk", page_id.0);

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let handles = self.file_handles.lock().unwrap();
        log::debug!("Syncing {} data files to disk", handles.len());
        for file in handles.values() {
            file.sync_all()?;
        }
        Ok(())
    }

    fn page_file_path(&self, page_id: PageId) -> PathBuf {
        let file_num = page_id.0 / 1000;
        self.data_dir.join(format!("data_{}.db", file_num))
    }

    fn page_offset(&self, page_id: PageId) -> u64 {
        let page_in_file = page_id.0 % 1000;
        (page_in_file as u64) * (PAGE_SIZE as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_disk_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let _dm = DiskManager::new(temp_dir.path()).unwrap();
        assert!(temp_dir.path().exists());
    }

    #[test]
    fn test_write_and_read_page() {
        let temp_dir = TempDir::new().unwrap();
        let dm = DiskManager::new(temp_dir.path()).unwrap();

        let page_id = PageId(0);
        let mut page = Page::new(page_id);
        let data_to_write = vec![1, 2, 3, 4, 5, 6, 7, 8];
        page.set_data(data_to_write.clone());

        dm.write_page(page_id, &page).unwrap();
        let read_page = dm.read_page(page_id).unwrap();

        assert_eq!(read_page.id(), page_id);
        assert_eq!(read_page.data()[16..16 + data_to_write.len()], data_to_write[..]);
    }

    #[test]
    fn test_multiple_pages_in_different_files() {
        let temp_dir = TempDir::new().unwrap();
        let dm = DiskManager::new(temp_dir.path()).unwrap();

        // Page in the first file
        let page_id_1 = PageId(0);
        let mut page_1 = Page::new(page_id_1);
        let data_1 = vec![10; 100];
        page_1.set_data(data_1.clone());
        dm.write_page(page_id_1, &page_1).unwrap();

        // Page in the same file, different offset
        let page_id_2 = PageId(1);
        let mut page_2 = Page::new(page_id_2);
        let data_2 = vec![20; 100];
        page_2.set_data(data_2.clone());
        dm.write_page(page_id_2, &page_2).unwrap();

        // Page in a different file
        let page_id_3 = PageId(1001); // Should be in data_1.db
        let mut page_3 = Page::new(page_id_3);
        let data_3 = vec![30; 100];
        page_3.set_data(data_3.clone());
        dm.write_page(page_id_3, &page_3).unwrap();

        // Read and verify
        let read_page_1 = dm.read_page(page_id_1).unwrap();
        assert_eq!(read_page_1.id(), page_id_1);
        assert_eq!(read_page_1.data()[16..16 + data_1.len()], data_1[..]);

        let read_page_2 = dm.read_page(page_id_2).unwrap();
        assert_eq!(read_page_2.id(), page_id_2);
        assert_eq!(read_page_2.data()[16..16 + data_2.len()], data_2[..]);

        let read_page_3 = dm.read_page(page_id_3).unwrap();
        assert_eq!(read_page_3.id(), page_id_3);
        assert_eq!(read_page_3.data()[16..16 + data_3.len()], data_3[..]);
    }

    #[test]
    fn test_sync() {
        let temp_dir = TempDir::new().unwrap();
        let dm = DiskManager::new(temp_dir.path()).unwrap();

        let page_id = PageId(0);
        let page = Page::new(page_id);

        dm.write_page(page_id, &page).unwrap();
        dm.sync().unwrap();
    }
}
