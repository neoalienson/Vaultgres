use super::error::Result;

/// Page size in bytes (8KB)
pub const PAGE_SIZE: usize = 8192;

/// Page identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u32);

/// Page header structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub page_id: u32,
    pub checksum: u32,
    pub flags: u16,
    pub lower: u16,  // End of item array
    pub upper: u16,  // Start of free space
    pub special: u16, // Special space offset
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
}

/// Database page with 8KB fixed size
#[derive(Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Creates a new empty page
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: [0; PAGE_SIZE],
        };
        let header = PageHeader::new(page_id);
        page.write_header(&header);
        page
    }
    
    /// Returns the page ID
    pub fn id(&self) -> PageId {
        let header = self.header();
        PageId(header.page_id)
    }
    
    /// Returns the page header
    pub fn header(&self) -> PageHeader {
        unsafe {
            std::ptr::read(self.data.as_ptr() as *const PageHeader)
        }
    }
    
    /// Writes the page header
    fn write_header(&mut self, header: &PageHeader) {
        unsafe {
            std::ptr::write(self.data.as_mut_ptr() as *mut PageHeader, *header);
        }
    }
    
    /// Returns available free space in bytes
    pub fn free_space(&self) -> usize {
        let header = self.header();
        (header.upper - header.lower) as usize
    }
    
    /// Returns raw page data
    pub fn data(&self) -> &[u8] {
        &self.data
    }
    
    /// Returns mutable raw page data
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    /// Creates page from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(bytes);
        Self { data }
    }
    
    /// Converts page to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.to_vec()
    }
    
    /// Sets page data (for testing)
    pub fn set_data(&mut self, new_data: Vec<u8>) {
        let start = PageHeader::SIZE;
        let len = new_data.len().min(PAGE_SIZE - start);
        self.data[start..start + len].copy_from_slice(&new_data[..len]);
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
}
