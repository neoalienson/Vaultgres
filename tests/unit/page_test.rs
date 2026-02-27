use rustgres::storage::{Page, PageId, PAGE_SIZE};

#[test]
fn test_page_creation() {
    let page = Page::new(PageId(42));
    assert_eq!(page.id(), PageId(42));
}

#[test]
fn test_page_size() {
    let page = Page::new(PageId(1));
    assert_eq!(page.data().len(), PAGE_SIZE);
}

#[test]
fn test_page_free_space() {
    let page = Page::new(PageId(1));
    let free = page.free_space();
    assert!(free > 0);
    assert!(free < PAGE_SIZE);
}

#[test]
fn test_page_serialization() {
    let page = Page::new(PageId(10));
    let bytes = page.to_bytes();
    
    assert_eq!(bytes.len(), PAGE_SIZE);
    
    let restored = Page::from_bytes(&bytes);
    assert_eq!(restored.id(), PageId(10));
}

#[test]
fn test_page_set_data() {
    let mut page = Page::new(PageId(1));
    let test_data = vec![1, 2, 3, 4, 5];
    
    page.set_data(test_data.clone());
    
    // Data should be written after header
    let page_bytes = page.to_bytes();
    assert!(page_bytes.len() == PAGE_SIZE);
}

#[test]
fn test_page_id_equality() {
    let id1 = PageId(100);
    let id2 = PageId(100);
    let id3 = PageId(200);
    
    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_multiple_pages() {
    let pages: Vec<Page> = (0..10)
        .map(|i| Page::new(PageId(i)))
        .collect();
    
    assert_eq!(pages.len(), 10);
    
    for (i, page) in pages.iter().enumerate() {
        assert_eq!(page.id(), PageId(i as u32));
    }
}
