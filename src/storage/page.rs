use crate::storage::compression::{
    COMPRESSED_HEADER_SIZE, CompressionAlgorithm, CompressionError, compress, decompress,
    should_compress,
};

pub const PAGE_SIZE: usize = 8192;
const PAGE_COMPRESSED_FLAG: u16 = 0x0001;
const ITEM_COMPRESSED_FLAG: u8 = 0x80;
const ITEM_ALGO_MASK: u8 = 0x03;
const MAX_ITEM_SIZE: usize = PAGE_SIZE - 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u32);

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub page_id: u32,
    pub checksum: u32,
    pub flags: u16,
    pub lower: u16,
    pub upper: u16,
    pub special: u16,
}

impl PageHeader {
    const SIZE: usize = 16;

    fn new(page_id: PageId) -> Self {
        Self {
            page_id: page_id.0,
            checksum: 0,
            flags: 0,
            lower: Self::SIZE as u16,
            upper: PAGE_SIZE as u16,
            special: PAGE_SIZE as u16,
        }
    }

    pub fn is_compressed(&self) -> bool {
        (self.flags & PAGE_COMPRESSED_FLAG) != 0
    }

    pub fn set_compressed(&mut self) {
        self.flags |= PAGE_COMPRESSED_FLAG;
    }

    pub fn clear_compressed(&mut self) {
        self.flags &= !PAGE_COMPRESSED_FLAG;
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ItemId {
    pub offset: u16,
    pub length: u16,
}

impl ItemId {
    const SIZE: usize = 4;

    pub fn new(offset: u16, length: u16) -> Self {
        Self { offset, length }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ItemHeader {
    pub offset: u16,
    pub length: u16,
    pub flags: u8,
}

impl ItemHeader {
    const SIZE: usize = 5;

    pub fn new(offset: u16, length: u16) -> Self {
        Self { offset, length, flags: 0 }
    }

    pub fn is_compressed(&self) -> bool {
        (self.flags & ITEM_COMPRESSED_FLAG) != 0
    }

    pub fn set_compressed(&mut self, algorithm: CompressionAlgorithm) {
        self.flags |= ITEM_COMPRESSED_FLAG;
        let algo_bits = match algorithm {
            CompressionAlgorithm::None => 0x00,
            CompressionAlgorithm::Lz4 => 0x01,
            CompressionAlgorithm::Zstd => 0x02,
        };
        self.flags = (self.flags & !ITEM_ALGO_MASK) | algo_bits;
    }

    pub fn clear_compressed(&mut self) {
        self.flags &= !ITEM_COMPRESSED_FLAG;
        self.flags &= !ITEM_ALGO_MASK;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        if !self.is_compressed() {
            return CompressionAlgorithm::None;
        }
        match self.flags & ITEM_ALGO_MASK {
            0x01 => CompressionAlgorithm::Lz4,
            0x02 => CompressionAlgorithm::Zstd,
            _ => CompressionAlgorithm::None,
        }
    }

    pub fn original_size(&self, item_data: &[u8]) -> Option<usize> {
        if !self.is_compressed() {
            return None;
        }
        if item_data.len() < 4 {
            return None;
        }
        let size_bytes = &item_data[item_data.len() - 4..];
        Some(u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize)
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self { data: [0; PAGE_SIZE] };
        let header = PageHeader::new(page_id);
        page.write_header(&header);
        page
    }

    pub fn id(&self) -> PageId {
        let header = self.header();
        PageId(header.page_id)
    }

    pub fn header(&self) -> PageHeader {
        unsafe { std::ptr::read(self.data.as_ptr() as *const PageHeader) }
    }

    fn write_header(&mut self, header: &PageHeader) {
        unsafe { std::ptr::write(self.data.as_mut_ptr() as *mut PageHeader, *header) }
    }

    pub fn free_space(&self) -> usize {
        let header = self.header();
        (header.upper - header.lower) as usize
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(bytes);
        Self { data }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    pub fn set_data(&mut self, new_data: Vec<u8>) {
        let start = PageHeader::SIZE;
        let len = new_data.len().min(PAGE_SIZE - start);
        self.data[start..start + len].copy_from_slice(&new_data[..len]);
    }

    pub fn is_compressed(&self) -> bool {
        self.header().is_compressed()
    }

    pub fn compress(&mut self, algorithm: CompressionAlgorithm) -> Result<(), CompressionError> {
        if self.is_compressed() {
            return Ok(());
        }

        let header = self.header();

        let item_ids_start = PageHeader::SIZE;
        let item_ids_end = header.lower as usize;
        let item_data_start = header.upper as usize;
        let item_data_end = PAGE_SIZE;

        let item_ids_size = item_ids_end - item_ids_start;
        let item_data_size = item_data_end - item_data_start;

        if item_ids_size == 0 && item_data_size == 0 {
            let mut new_header = header;
            new_header.set_compressed();
            self.write_header(&new_header);
            return Ok(());
        }

        let mut to_compress = Vec::with_capacity(4 + item_ids_size + item_data_size);
        to_compress.extend_from_slice(&(header.lower as u32).to_le_bytes());
        to_compress.extend_from_slice(&(header.upper as u32).to_le_bytes());
        to_compress.extend_from_slice(&self.data[item_ids_start..item_ids_end]);
        to_compress.extend_from_slice(&self.data[item_data_start..item_data_end]);

        let compressed = compress(&to_compress, algorithm)?;

        if compressed.len() >= to_compress.len() {
            let mut new_header = header;
            new_header.set_compressed();
            self.write_header(&new_header);
            return Ok(());
        }

        let mut new_header = header;
        new_header.set_compressed();

        self.data[PageHeader::SIZE..].fill(0);

        let compressed_len = compressed.len();
        let copy_start = PAGE_SIZE - compressed_len - 4;

        self.data[PAGE_SIZE - 4..PAGE_SIZE]
            .copy_from_slice(&(to_compress.len() as u32).to_le_bytes());
        self.data[copy_start..copy_start + compressed_len].copy_from_slice(&compressed);

        new_header.special = copy_start as u16;

        self.write_header(&new_header);

        Ok(())
    }

    pub fn decompress(&mut self, algorithm: CompressionAlgorithm) -> Result<(), CompressionError> {
        if !self.is_compressed() {
            return Ok(());
        }

        let header = self.header();
        let compressed_start = header.special as usize;

        if compressed_start <= PageHeader::SIZE || compressed_start >= PAGE_SIZE - 4 {
            let mut new_header = header;
            new_header.clear_compressed();
            new_header.lower = PageHeader::SIZE as u16;
            new_header.upper = PAGE_SIZE as u16;
            self.write_header(&new_header);
            return Ok(());
        }

        let compressed_len = PAGE_SIZE - compressed_start - 4;

        let original_size_bytes = &self.data[PAGE_SIZE - 4..PAGE_SIZE];
        let original_size = u32::from_le_bytes(original_size_bytes.try_into().unwrap()) as usize;

        let compressed_data = &self.data[compressed_start..compressed_start + compressed_len];

        let decompressed = decompress(compressed_data, algorithm, original_size)?;

        if decompressed.len() < 8 {
            let mut new_header = header;
            new_header.clear_compressed();
            new_header.lower = PageHeader::SIZE as u16;
            new_header.upper = PAGE_SIZE as u16;
            self.write_header(&new_header);
            return Ok(());
        }

        let original_lower = u32::from_le_bytes(decompressed[..4].try_into().unwrap()) as usize;
        let original_upper = u32::from_le_bytes(decompressed[4..8].try_into().unwrap()) as usize;

        let item_ids_size = original_lower - PageHeader::SIZE;
        let item_data_size = PAGE_SIZE - original_upper;

        if decompressed.len() < 8 + item_ids_size + item_data_size {
            let mut new_header = header;
            new_header.clear_compressed();
            new_header.lower = PageHeader::SIZE as u16;
            new_header.upper = PAGE_SIZE as u16;
            self.write_header(&new_header);
            return Ok(());
        }

        let mut new_header = header;
        new_header.clear_compressed();
        new_header.lower = original_lower as u16;
        new_header.upper = original_upper as u16;

        let item_ids_end = PageHeader::SIZE + item_ids_size;
        self.data[PageHeader::SIZE..item_ids_end]
            .copy_from_slice(&decompressed[8..8 + item_ids_size]);

        let item_data_start = PAGE_SIZE - item_data_size;
        self.data[item_data_start..PAGE_SIZE].copy_from_slice(&decompressed[8 + item_ids_size..]);

        self.write_header(&new_header);

        Ok(())
    }

    pub fn item_id_count(&self) -> usize {
        let header = self.header();
        let item_ids_start = PageHeader::SIZE as u16;
        ((header.lower - item_ids_start) as usize) / ItemId::SIZE
    }

    pub fn get_item_id(&self, index: usize) -> Option<ItemId> {
        let header = self.header();
        let item_ids_start = PageHeader::SIZE;
        let item_ids_end = header.lower as usize;

        if item_ids_start + (index + 1) * ItemId::SIZE > item_ids_end {
            return None;
        }

        let offset = item_ids_start + index * ItemId::SIZE;
        let bytes = &self.data[offset..offset + ItemId::SIZE];
        Some(unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const ItemId) })
    }

    fn write_item_id(&mut self, index: usize, item_id: &ItemId) {
        let header = self.header();
        let item_ids_start = PageHeader::SIZE;
        let offset = item_ids_start + index * ItemId::SIZE;

        let bytes: [u8; ItemId::SIZE] = unsafe { std::mem::transmute(*item_id) };
        self.data[offset..offset + ItemId::SIZE].copy_from_slice(&bytes);
    }

    pub fn item_count(&self) -> usize {
        self.item_id_count()
    }

    pub fn get_item(&self, index: usize) -> Option<(ItemHeader, Vec<u8>)> {
        let item_id = self.get_item_id(index)?;

        let data_offset = item_id.offset as usize;
        let item_length = item_id.length as usize;

        if data_offset + item_length > PAGE_SIZE {
            return None;
        }

        let item_data = &self.data[data_offset..data_offset + item_length];

        let is_compressed = (item_data[0] & ITEM_COMPRESSED_FLAG) != 0;

        if is_compressed {
            let algo = match item_data[0] & ITEM_ALGO_MASK {
                0x01 => CompressionAlgorithm::Lz4,
                0x02 => CompressionAlgorithm::Zstd,
                _ => CompressionAlgorithm::None,
            };

            let compressed_size = item_length.saturating_sub(COMPRESSED_HEADER_SIZE);
            let compressed_data = &item_data[1..1 + compressed_size];
            let original_size_bytes = &item_data[1 + compressed_size..item_length];

            if original_size_bytes.len() != 4 {
                return None;
            }

            let original_size =
                u32::from_le_bytes(original_size_bytes.try_into().unwrap()) as usize;
            let decompressed = decompress(compressed_data, algo, original_size).ok()?;

            Some((ItemHeader::new(item_id.offset, item_id.length), decompressed))
        } else {
            Some((ItemHeader::new(item_id.offset, item_id.length), item_data.to_vec()))
        }
    }

    pub fn add_item(
        &mut self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<usize, CompressionError> {
        let header = self.header();

        let (item_data, is_compressed) =
            if should_compress(data.len()) && algorithm != CompressionAlgorithm::None {
                let compressed = compress(data, algorithm)?;
                if compressed.len() < data.len() {
                    let mut full_data = Vec::with_capacity(1 + compressed.len() + 4);
                    let mut flags = ITEM_COMPRESSED_FLAG;
                    let algo_bits = match algorithm {
                        CompressionAlgorithm::Lz4 => 0x01,
                        CompressionAlgorithm::Zstd => 0x02,
                        CompressionAlgorithm::None => 0x00,
                    };
                    flags |= algo_bits;
                    full_data.push(flags);
                    full_data.extend_from_slice(&compressed);
                    full_data.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    (full_data, true)
                } else {
                    (data.to_vec(), false)
                }
            } else {
                (data.to_vec(), false)
            };

        let item_size = ItemId::SIZE + item_data.len();
        if item_size > self.free_space() {
            return Err(CompressionError::InvalidData);
        }

        let mut new_header = header;

        new_header.upper = (new_header.upper as usize - item_data.len()) as u16;
        let data_offset = new_header.upper as usize;

        let item_id_index = self.item_id_count();
        let item_id_offset = PageHeader::SIZE + item_id_index * ItemId::SIZE;
        new_header.lower = (item_id_offset + ItemId::SIZE) as u16;

        self.data[data_offset..data_offset + item_data.len()].copy_from_slice(&item_data);

        let item_id = ItemId::new(data_offset as u16, item_data.len() as u16);
        self.write_item_id(item_id_index, &item_id);

        self.write_header(&new_header);

        Ok(item_id_index)
    }

    pub fn remove_item(&mut self, index: usize) -> Result<(), CompressionError> {
        let item_count = self.item_id_count();
        if index >= item_count {
            return Err(CompressionError::InvalidData);
        }

        let header = self.header();
        let mut new_header = header;

        let last_index = item_count - 1;

        for i in index..last_index {
            let next_item_id = self.get_item_id(i + 1).unwrap();
            self.write_item_id(i, &next_item_id);
        }

        let last_item_id = self.get_item_id(last_index).unwrap();
        let item_size = last_item_id.length as usize;

        new_header.lower = (header.lower as usize - ItemId::SIZE) as u16;

        self.write_header(&new_header);

        Ok(())
    }

    pub fn clear(&mut self) {
        let header = self.header();
        let mut new_header = header;
        new_header.lower = PageHeader::SIZE as u16;
        new_header.upper = PAGE_SIZE as u16;
        new_header.flags = 0;
        self.write_header(&new_header);
        self.data[PageHeader::SIZE..].fill(0);
    }

    pub fn items(&self) -> Vec<Vec<u8>> {
        let count = self.item_count();
        (0..count).filter_map(|i| self.get_item(i).map(|(_, data)| data)).collect()
    }

    pub fn get_raw_item(&self, index: usize) -> Option<(ItemId, &[u8])> {
        let item_id = self.get_item_id(index)?;
        let offset = item_id.offset as usize;
        let length = item_id.length as usize;
        Some((item_id, &self.data[offset..offset + length]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(PageId(1));
        assert_eq!(page.id(), PageId(1));
        assert_eq!(page.data.len(), PAGE_SIZE);
    }

    #[test]
    fn test_page_free_space() {
        let page = Page::new(PageId(1));
        let expected_free = PAGE_SIZE - PageHeader::SIZE;
        assert_eq!(page.free_space(), expected_free);
    }

    #[test]
    fn test_page_header() {
        let page = Page::new(PageId(42));
        let header = page.header();
        assert_eq!(header.page_id, 42);
        assert_eq!(header.lower, PageHeader::SIZE as u16);
        assert_eq!(header.upper, PAGE_SIZE as u16);
    }

    #[test]
    fn test_page_not_compressed_by_default() {
        let page = Page::new(PageId(1));
        assert!(!page.is_compressed());
    }

    #[test]
    fn test_item_header_compressed_flag() {
        let mut item_header = ItemHeader::new(100, 50);
        assert!(!item_header.is_compressed());

        item_header.set_compressed(CompressionAlgorithm::Lz4);
        assert!(item_header.is_compressed());
        assert_eq!(item_header.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_item_header_zstd_compression() {
        let mut item_header = ItemHeader::new(100, 50);
        item_header.set_compressed(CompressionAlgorithm::Zstd);
        assert!(item_header.is_compressed());
        assert_eq!(item_header.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_item_header_clear_compressed() {
        let mut item_header = ItemHeader::new(100, 50);
        item_header.set_compressed(CompressionAlgorithm::Lz4);
        item_header.clear_compressed();
        assert!(!item_header.is_compressed());
        assert_eq!(item_header.compression_algorithm(), CompressionAlgorithm::None);
    }

    #[test]
    fn test_add_and_get_item() {
        let mut page = Page::new(PageId(1));
        let data = vec![1u8, 2, 3, 4, 5];

        let index = page.add_item(&data, CompressionAlgorithm::Lz4).unwrap();
        assert_eq!(index, 0);

        let (_, retrieved) = page.get_item(0).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_add_multiple_items() {
        let mut page = Page::new(PageId(1));

        for i in 0..5 {
            let data = vec![i as u8; 10];
            page.add_item(&data, CompressionAlgorithm::Lz4).unwrap();
        }

        assert_eq!(page.item_count(), 5);

        for i in 0..5 {
            let (_, data) = page.get_item(i).unwrap();
            assert_eq!(data, vec![i as u8; 10]);
        }
    }

    #[test]
    fn test_remove_item() {
        let mut page = Page::new(PageId(1));

        let data0 = vec![0u8; 10];
        let data1 = vec![1u8; 10];
        let data2 = vec![2u8; 10];

        page.add_item(&data0, CompressionAlgorithm::Lz4).unwrap();
        page.add_item(&data1, CompressionAlgorithm::Lz4).unwrap();
        page.add_item(&data2, CompressionAlgorithm::Lz4).unwrap();

        assert_eq!(page.item_count(), 3);

        page.remove_item(1).unwrap();

        assert_eq!(page.item_count(), 2);

        let (_, data) = page.get_item(0).unwrap();
        assert_eq!(data, data0);

        let (_, data) = page.get_item(1).unwrap();
        assert_eq!(data, data2);
    }

    #[test]
    fn test_remove_first_item() {
        let mut page = Page::new(PageId(1));

        let data0 = vec![10u8; 10];
        let data1 = vec![20u8; 10];
        let data2 = vec![30u8; 10];

        page.add_item(&data0, CompressionAlgorithm::None).unwrap();
        page.add_item(&data1, CompressionAlgorithm::None).unwrap();
        page.add_item(&data2, CompressionAlgorithm::None).unwrap();

        assert_eq!(page.item_count(), 3);

        page.remove_item(0).unwrap();

        assert_eq!(page.item_count(), 2);

        let (_, data) = page.get_item(0).unwrap();
        assert_eq!(data, data1, "First item after removal should be data1");

        let (_, data) = page.get_item(1).unwrap();
        assert_eq!(data, data2, "Second item after removal should be data2");
    }

    #[test]
    fn test_remove_last_item() {
        let mut page = Page::new(PageId(1));

        let data0 = vec![10u8; 10];
        let data1 = vec![20u8; 10];
        let data2 = vec![30u8; 10];

        page.add_item(&data0, CompressionAlgorithm::None).unwrap();
        page.add_item(&data1, CompressionAlgorithm::None).unwrap();
        page.add_item(&data2, CompressionAlgorithm::None).unwrap();

        assert_eq!(page.item_count(), 3);

        page.remove_item(2).unwrap();

        assert_eq!(page.item_count(), 2);

        let (_, data) = page.get_item(0).unwrap();
        assert_eq!(data, data0);

        let (_, data) = page.get_item(1).unwrap();
        assert_eq!(data, data1);
    }

    #[test]
    fn test_remove_all_items_one_by_one() {
        let mut page = Page::new(PageId(1));

        let data0 = vec![10u8; 10];
        let data1 = vec![20u8; 10];
        let data2 = vec![30u8; 10];

        page.add_item(&data0, CompressionAlgorithm::None).unwrap();
        page.add_item(&data1, CompressionAlgorithm::None).unwrap();
        page.add_item(&data2, CompressionAlgorithm::None).unwrap();

        page.remove_item(0).unwrap();
        assert_eq!(page.item_count(), 2);
        assert_eq!(page.get_item(0).unwrap().1, data1);
        assert_eq!(page.get_item(1).unwrap().1, data2);

        page.remove_item(0).unwrap();
        assert_eq!(page.item_count(), 1);
        assert_eq!(page.get_item(0).unwrap().1, data2);

        page.remove_item(0).unwrap();
        assert_eq!(page.item_count(), 0);
    }

    #[test]
    fn test_page_clear() {
        let mut page = Page::new(PageId(1));
        let data = vec![1u8, 2, 3, 4, 5];

        page.add_item(&data, CompressionAlgorithm::Lz4).unwrap();
        assert_eq!(page.item_count(), 1);

        page.clear();
        assert_eq!(page.item_count(), 0);
        assert!(!page.is_compressed());
    }

    #[test]
    fn test_compress_page() {
        let mut page = Page::new(PageId(1));

        for _ in 0..10 {
            let data = vec![0u8; 200];
            page.add_item(&data, CompressionAlgorithm::Lz4).unwrap();
        }

        assert!(!page.is_compressed());

        page.compress(CompressionAlgorithm::Lz4).unwrap();

        assert!(page.is_compressed());
    }

    #[test]
    fn test_decompress_page() {
        let mut page = Page::new(PageId(1));

        for _ in 0..10 {
            let data = vec![0u8; 200];
            page.add_item(&data, CompressionAlgorithm::Lz4).unwrap();
        }

        page.compress(CompressionAlgorithm::Lz4).unwrap();
        assert!(page.is_compressed());

        page.decompress(CompressionAlgorithm::Lz4).unwrap();
        assert!(!page.is_compressed());
        assert_eq!(page.item_count(), 10);
    }

    #[test]
    fn test_compressed_item_roundtrip() {
        let mut page = Page::new(PageId(1));

        let large_data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();
        page.add_item(&large_data, CompressionAlgorithm::Lz4).unwrap();

        let item_count = page.item_count();
        assert_eq!(item_count, 1);

        let small_data = vec![1u8, 2, 3];
        page.add_item(&small_data, CompressionAlgorithm::Lz4).unwrap();

        let (_, retrieved) = page.get_item(1).unwrap();
        assert_eq!(retrieved, small_data);
    }

    #[test]
    fn test_small_item_not_compressed() {
        let mut page = Page::new(PageId(1));

        let small_data = vec![1u8, 2, 3];
        page.add_item(&small_data, CompressionAlgorithm::Lz4).unwrap();

        let (header, _) = page.get_raw_item(0).unwrap();
        let is_compressed = (header.length as usize) > small_data.len();
        assert!(!is_compressed || header.length as usize == small_data.len() + 4);
    }

    #[test]
    fn test_page_items() {
        let mut page = Page::new(PageId(1));

        let items = vec![vec![1u8, 2, 3], vec![4u8, 5, 6], vec![7u8, 8, 9]];

        for item in &items {
            page.add_item(item, CompressionAlgorithm::Lz4).unwrap();
        }

        let retrieved_items = page.items();
        assert_eq!(retrieved_items, items);
    }

    #[test]
    fn test_get_item_out_of_bounds() {
        let page = Page::new(PageId(1));
        assert!(page.get_item(0).is_none());
    }

    #[test]
    fn test_empty_page_compression() {
        let mut page = Page::new(PageId(1));

        page.compress(CompressionAlgorithm::Lz4).unwrap();
        assert!(page.is_compressed());

        page.decompress(CompressionAlgorithm::Lz4).unwrap();
        assert!(!page.is_compressed());

        assert_eq!(page.item_count(), 0);
    }

    #[test]
    fn test_page_header_compressed_flag() {
        let mut page = Page::new(PageId(1));
        assert!(!page.is_compressed());

        let mut header = page.header();
        header.set_compressed();
        page.write_header(&header);
        assert!(page.is_compressed());

        let mut header = page.header();
        header.clear_compressed();
        page.write_header(&header);
        assert!(!page.is_compressed());
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let mut page = Page::new(PageId(1));

        for _ in 0..4 {
            let data: Vec<u8> = (0..2000).map(|i| ((i * 7) % 256) as u8).collect();
            page.add_item(&data, CompressionAlgorithm::Zstd).unwrap();
        }

        assert_eq!(page.item_count(), 4);

        page.compress(CompressionAlgorithm::Zstd).unwrap();
        assert!(page.is_compressed());

        page.decompress(CompressionAlgorithm::Zstd).unwrap();
        assert!(!page.is_compressed());
        assert_eq!(page.item_count(), 4);
    }

    #[test]
    fn test_compression_threshold() {
        let small_data = vec![0u8; 100];
        assert!(!should_compress(small_data.len()));

        let large_data = vec![0u8; 3000];
        assert!(should_compress(large_data.len()));
    }

    #[test]
    fn test_compressed_item_with_different_algorithms() {
        let mut page = Page::new(PageId(1));

        let large_data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();

        page.add_item(&large_data, CompressionAlgorithm::Lz4).unwrap();
        let (_, retrieved) = page.get_item(0).unwrap();
        assert_eq!(retrieved, large_data);

        page.add_item(&large_data, CompressionAlgorithm::Zstd).unwrap();
        let (_, retrieved) = page.get_item(1).unwrap();
        assert_eq!(retrieved, large_data);

        page.add_item(&large_data, CompressionAlgorithm::None).unwrap();
        let (_, retrieved) = page.get_item(2).unwrap();
        assert_eq!(retrieved, large_data);
    }

    #[test]
    fn test_items_after_decompression() {
        let mut page = Page::new(PageId(1));

        let items: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8; 100]).collect();
        for item in &items {
            page.add_item(item, CompressionAlgorithm::Lz4).unwrap();
        }

        page.compress(CompressionAlgorithm::Lz4).unwrap();
        page.decompress(CompressionAlgorithm::Lz4).unwrap();

        let retrieved_items = page.items();
        assert_eq!(retrieved_items.len(), items.len());
        for (i, item) in items.iter().enumerate() {
            assert_eq!(&retrieved_items[i], item);
        }
    }

    #[test]
    fn test_item_ids_after_removal() {
        let mut page = Page::new(PageId(1));

        page.add_item(&vec![10u8; 10], CompressionAlgorithm::None).unwrap();
        page.add_item(&vec![20u8; 10], CompressionAlgorithm::None).unwrap();
        page.add_item(&vec![30u8; 10], CompressionAlgorithm::None).unwrap();

        let id0_before = page.get_item_id(0);
        let id2_before = page.get_item_id(2);

        page.remove_item(1).unwrap();

        let id0_after = page.get_item_id(0);
        let id1_after = page.get_item_id(1);

        assert_eq!(id0_before.unwrap().offset, id0_after.unwrap().offset);
        assert_eq!(id2_before.unwrap().offset, id1_after.unwrap().offset);
        assert_eq!(page.item_count(), 2);
    }
}
