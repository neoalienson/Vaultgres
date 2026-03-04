use super::old_executor::{OldExecutor as Executor, OldExecutorError as ExecutorError, Tuple};
use crate::storage::heap::HeapFile;
use crate::storage::PageId;
use std::sync::Arc;

pub struct SeqScan {
    heap: Arc<HeapFile>,
    table_name: String,
    current_page: u32,
    current_slot: u16,
    total_pages: u32,
}

impl SeqScan {
    pub fn new(heap: Arc<HeapFile>, table_name: String) -> Self {
        Self { heap, table_name, current_page: 0, current_slot: 0, total_pages: 1 }
    }
}

impl Executor for SeqScan {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.current_page = 0;
        self.current_slot = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        loop {
            if self.current_page >= self.total_pages {
                return Ok(None);
            }

            let page_id = PageId(self.current_page);
            match self.heap.get_tuple(page_id, self.current_slot) {
                Ok(data) => {
                    if data.is_empty() {
                        self.current_page += 1;
                        self.current_slot = 0;
                        if self.current_page >= self.total_pages {
                            return Ok(None);
                        }
                        continue;
                    }
                    self.current_slot += 1;
                    let mut tuple = Tuple::new();
                    tuple.insert(self.table_name.clone(), data);
                    return Ok(Some(tuple));
                }
                Err(_) => {
                    self.current_page += 1;
                    self.current_slot = 0;
                    if self.current_page >= self.total_pages {
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BufferPool;

    #[test]
    fn test_seq_scan_empty() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = Arc::new(HeapFile::new(pool));
        let mut scan = SeqScan::new(heap, "test".to_string());

        scan.open().unwrap();
        assert!(scan.next().unwrap().is_none());
        scan.close().unwrap();
    }

    #[test]
    fn test_seq_scan_with_data() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = Arc::new(HeapFile::new(pool));

        heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();
        heap.insert_tuple(PageId(0), vec![4, 5, 6]).unwrap();

        let mut scan = SeqScan::new(heap, "test".to_string());
        scan.open().unwrap();

        let t1 = scan.next().unwrap().unwrap();
        assert_eq!(t1.get("test").unwrap(), &vec![1, 2, 3]);

        let t2 = scan.next().unwrap().unwrap();
        assert_eq!(t2.get("test").unwrap(), &vec![4, 5, 6]);

        assert!(scan.next().unwrap().is_none());
        scan.close().unwrap();
    }
}
