use super::error::Result;
use super::page::{Page, PageId};
use std::collections::HashMap;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{Read, Write, Seek, SeekFrom};
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
        
        Ok(Self {
            data_dir,
            file_handles: Mutex::new(HashMap::new()),
        })
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let file_path = self.page_file_path(page_id);
        let mut handles = self.file_handles.lock().unwrap();
        
        let file = handles.entry(file_path.to_string_lossy().to_string())
            .or_insert_with(|| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&file_path)
                    .unwrap()
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
        
        let file = handles.entry(file_path.to_string_lossy().to_string())
            .or_insert_with(|| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&file_path)
                    .unwrap()
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
        let dm = DiskManager::new(temp_dir.path()).unwrap();
        assert!(temp_dir.path().exists());
    }

    #[test]
    fn test_write_and_read_page() {
        let temp_dir = TempDir::new().unwrap();
        let dm = DiskManager::new(temp_dir.path()).unwrap();
        
        let page_id = PageId(0);
        let mut page = Page::new(page_id);
        page.set_data(vec![1, 2, 3, 4]);
        
        dm.write_page(page_id, &page).unwrap();
        let read_page = dm.read_page(page_id).unwrap();
        
        assert_eq!(read_page.id(), page_id);
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
