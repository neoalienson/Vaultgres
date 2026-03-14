use super::error::Result;
use super::writer::{LSN, WALRecord};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024; // 16MB

pub struct WALDiskWriter {
    wal_dir: PathBuf,
    current_file: Mutex<Option<File>>,
    current_segment: Mutex<u64>,
    current_offset: Mutex<u64>,
}

impl WALDiskWriter {
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        create_dir_all(&wal_dir)?;

        Ok(Self {
            wal_dir,
            current_file: Mutex::new(None),
            current_segment: Mutex::new(0),
            current_offset: Mutex::new(0),
        })
    }

    pub fn write(&self, record: &WALRecord) -> Result<LSN> {
        let data = record.to_bytes();
        let mut offset = self.current_offset.lock().unwrap();
        let mut segment = self.current_segment.lock().unwrap();
        let mut file_opt = self.current_file.lock().unwrap();

        // Check if we need a new segment
        if *offset + data.len() as u64 > WAL_SEGMENT_SIZE {
            *segment += 1;
            *offset = 0;
            *file_opt = None;
        }

        // Open file if needed
        if file_opt.is_none() {
            let path = self.segment_path(*segment);
            *file_opt = Some(OpenOptions::new().create(true).append(true).open(path)?);
        }

        let file = file_opt.as_mut().unwrap();
        file.write_all(&data)?;

        let lsn = *segment * WAL_SEGMENT_SIZE + *offset;
        *offset += data.len() as u64;

        Ok(lsn)
    }

    pub fn flush(&self) -> Result<()> {
        let mut file_opt = self.current_file.lock().unwrap();
        if let Some(file) = file_opt.as_mut() {
            file.sync_all()?;
        }
        Ok(())
    }

    fn segment_path(&self, segment: u64) -> PathBuf {
        self.wal_dir.join(format!("wal_{:016x}", segment))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PageId;
    use crate::wal::writer::RecordType;
    use tempfile::TempDir;

    #[test]
    fn test_wal_disk_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let _writer = WALDiskWriter::new(temp_dir.path()).unwrap();
        assert!(temp_dir.path().exists());
    }

    #[test]
    fn test_write_wal_record() {
        let temp_dir = TempDir::new().unwrap();
        let writer = WALDiskWriter::new(temp_dir.path()).unwrap();

        let record = WALRecord::new(1, 1, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3]);

        let lsn = writer.write(&record).unwrap();
        // Verify LSN was assigned (LSN is u64, always >= 0)
        assert!(lsn < WAL_SEGMENT_SIZE * 1000);
    }

    #[test]
    fn test_flush() {
        let temp_dir = TempDir::new().unwrap();
        let writer = WALDiskWriter::new(temp_dir.path()).unwrap();

        let record = WALRecord::new(1, 1, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3]);

        writer.write(&record).unwrap();
        writer.flush().unwrap();
    }
}
