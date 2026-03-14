use super::index_trait::{Index, IndexError, IndexType, TupleId};

pub struct BRINIndex {
    ranges: Vec<BlockRange>,
    pages_per_range: usize,
}

struct BlockRange {
    start_page: u32,
    end_page: u32,
    min_value: Option<Vec<u8>>,
    max_value: Option<Vec<u8>>,
    tids: Vec<TupleId>,
}

impl BRINIndex {
    pub fn new(pages_per_range: usize) -> Self {
        Self { ranges: vec![], pages_per_range }
    }

    fn get_or_create_range(&mut self, page: u32) -> &mut BlockRange {
        let range_idx = (page as usize) / self.pages_per_range;

        while self.ranges.len() <= range_idx {
            let start = (self.ranges.len() * self.pages_per_range) as u32;
            let end = start + self.pages_per_range as u32;
            self.ranges.push(BlockRange {
                start_page: start,
                end_page: end,
                min_value: None,
                max_value: None,
                tids: vec![],
            });
        }

        &mut self.ranges[range_idx]
    }

    fn update_range_bounds(&mut self, page: u32, key: &[u8]) {
        let range = self.get_or_create_range(page);

        match &range.min_value {
            None => range.min_value = Some(key.to_vec()),
            Some(min) if key < min.as_slice() => range.min_value = Some(key.to_vec()),
            _ => {}
        }

        match &range.max_value {
            None => range.max_value = Some(key.to_vec()),
            Some(max) if key > max.as_slice() => range.max_value = Some(key.to_vec()),
            _ => {}
        }
    }
}

impl Index for BRINIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let page = tid.0.0;
        self.update_range_bounds(page, key);
        let range = self.get_or_create_range(page);
        range.tids.push(tid);
        Ok(())
    }

    fn delete(&mut self, _key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        let page = tid.0.0;
        let range_idx = (page as usize) / self.pages_per_range;

        if range_idx < self.ranges.len() {
            let range = &mut self.ranges[range_idx];
            if let Some(pos) = range.tids.iter().position(|&t| t == tid) {
                range.tids.remove(pos);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let mut result = vec![];

        for range in &self.ranges {
            let matches = match (&range.min_value, &range.max_value) {
                (Some(min), Some(max)) => key >= min.as_slice() && key <= max.as_slice(),
                _ => false,
            };

            if matches {
                result.extend_from_slice(&range.tids);
            }
        }

        if result.is_empty() { Err(IndexError::KeyNotFound) } else { Ok(result) }
    }

    fn range_search(&self, start: &[u8], end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let mut result = vec![];

        for range in &self.ranges {
            let overlaps = match (&range.min_value, &range.max_value) {
                (Some(min), Some(max)) => max.as_slice() >= start && min.as_slice() <= end,
                _ => false,
            };

            if overlaps {
                result.extend_from_slice(&range.tids);
            }
        }

        if result.is_empty() { Err(IndexError::KeyNotFound) } else { Ok(result) }
    }

    fn index_type(&self) -> IndexType {
        IndexType::BRIN
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    #[test]
    fn test_brin_insert_and_search() {
        let mut index = BRINIndex::new(128);
        let tid = (PageId(0), 0);

        index.insert(b"key1", tid).unwrap();
        let result = index.search(b"key1").unwrap();
        assert!(result.contains(&tid));
    }

    #[test]
    fn test_brin_range_search() {
        let mut index = BRINIndex::new(128);

        index.insert(b"a", (PageId(0), 0)).unwrap();
        index.insert(b"m", (PageId(0), 1)).unwrap();
        index.insert(b"z", (PageId(0), 2)).unwrap();

        let result = index.range_search(b"a", b"n").unwrap();
        assert!(result.len() >= 2);
    }

    #[test]
    fn test_brin_multiple_ranges() {
        let mut index = BRINIndex::new(2);

        index.insert(b"a", (PageId(0), 0)).unwrap();
        index.insert(b"z", (PageId(5), 0)).unwrap();

        assert_eq!(index.ranges.len(), 3);
    }

    #[test]
    fn test_brin_not_found() {
        let index = BRINIndex::new(128);
        assert!(index.search(b"nonexistent").is_err());
    }
}
