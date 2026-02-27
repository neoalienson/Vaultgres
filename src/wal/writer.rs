use super::error::{Result, WALError};
use crate::transaction::TransactionId;
use crate::storage::PageId;
use std::sync::Mutex;

/// Log Sequence Number
pub type LSN = u64;

/// WAL record type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Insert,
    Update,
    Delete,
    Commit,
    Abort,
    Checkpoint,
}

/// WAL record
#[derive(Debug, Clone)]
pub struct WALRecord {
    pub lsn: LSN,
    pub xid: TransactionId,
    pub record_type: RecordType,
    pub page_id: Option<PageId>,
    pub data: Vec<u8>,
}

impl WALRecord {
    /// Creates a new WAL record
    pub fn new(
        lsn: LSN,
        xid: TransactionId,
        record_type: RecordType,
        page_id: Option<PageId>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            lsn,
            xid,
            record_type,
            page_id,
            data,
        }
    }
    
    /// Serializes the record to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.lsn.to_le_bytes());
        bytes.extend_from_slice(&self.xid.to_le_bytes());
        bytes.push(self.record_type as u8);
        
        if let Some(page_id) = self.page_id {
            bytes.push(1);
            bytes.extend_from_slice(&page_id.0.to_le_bytes());
        } else {
            bytes.push(0);
        }
        
        bytes.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.data);
        
        bytes
    }
}

/// WAL writer
pub struct WALWriter {
    next_lsn: Mutex<LSN>,
    buffer: Mutex<Vec<WALRecord>>,
    flushed_lsn: Mutex<LSN>,
}

impl WALWriter {
    /// Creates a new WAL writer
    pub fn new() -> Self {
        Self {
            next_lsn: Mutex::new(1),
            buffer: Mutex::new(Vec::new()),
            flushed_lsn: Mutex::new(0),
        }
    }
    
    /// Writes a WAL record
    pub fn write(
        &self,
        xid: TransactionId,
        record_type: RecordType,
        page_id: Option<PageId>,
        data: Vec<u8>,
    ) -> Result<LSN> {
        let mut next_lsn = self.next_lsn.lock().unwrap();
        let lsn = *next_lsn;
        *next_lsn += 1;
        drop(next_lsn);
        
        let record = WALRecord::new(lsn, xid, record_type, page_id, data);
        
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(record);
        
        log::trace!("WAL write: LSN={}, XID={}, type={:?}", lsn, xid, record_type);
        
        Ok(lsn)
    }
    
    /// Flushes WAL buffer to disk
    pub fn flush(&self) -> Result<LSN> {
        let mut buffer = self.buffer.lock().unwrap();
        
        if buffer.is_empty() {
            return Ok(*self.flushed_lsn.lock().unwrap());
        }
        
        // Simulate disk write
        let last_lsn = buffer.last().map(|r| r.lsn).unwrap_or(0);
        let count = buffer.len();
        
        buffer.clear();
        
        let mut flushed_lsn = self.flushed_lsn.lock().unwrap();
        *flushed_lsn = last_lsn;
        
        log::debug!("WAL flushed: {} records, LSN up to {}", count, last_lsn);
        
        Ok(last_lsn)
    }
    
    /// Gets the current LSN
    pub fn current_lsn(&self) -> LSN {
        *self.next_lsn.lock().unwrap() - 1
    }
    
    /// Gets the flushed LSN
    pub fn flushed_lsn(&self) -> LSN {
        *self.flushed_lsn.lock().unwrap()
    }
    
    /// Gets all records (for testing/recovery)
    pub fn get_records(&self) -> Vec<WALRecord> {
        self.buffer.lock().unwrap().clone()
    }
}

impl Default for WALWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_record_creation() {
        let record = WALRecord::new(
            1,
            10,
            RecordType::Insert,
            Some(PageId(1)),
            vec![1, 2, 3],
        );
        
        assert_eq!(record.lsn, 1);
        assert_eq!(record.xid, 10);
        assert_eq!(record.record_type, RecordType::Insert);
    }
    
    #[test]
    fn test_wal_record_serialization() {
        let record = WALRecord::new(
            1,
            10,
            RecordType::Insert,
            Some(PageId(1)),
            vec![1, 2, 3],
        );
        
        let bytes = record.to_bytes();
        assert!(!bytes.is_empty());
    }
    
    #[test]
    fn test_wal_writer_write() {
        let writer = WALWriter::new();
        
        let lsn = writer.write(10, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3])
            .unwrap();
        
        assert_eq!(lsn, 1);
        assert_eq!(writer.current_lsn(), 1);
    }
    
    #[test]
    fn test_wal_writer_flush() {
        let writer = WALWriter::new();
        
        writer.write(10, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3])
            .unwrap();
        
        let flushed_lsn = writer.flush().unwrap();
        assert_eq!(flushed_lsn, 1);
        assert_eq!(writer.flushed_lsn(), 1);
    }
    
    #[test]
    fn test_wal_writer_multiple_records() {
        let writer = WALWriter::new();
        
        let lsn1 = writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
            .unwrap();
        let lsn2 = writer.write(10, RecordType::Update, Some(PageId(2)), vec![])
            .unwrap();
        
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
    }
}
