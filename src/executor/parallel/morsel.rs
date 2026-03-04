use crate::executor::old_executor::SimpleTuple;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct Morsel {
    pub tuples: Vec<SimpleTuple>,
    pub start_offset: usize,
    pub end_offset: usize,
    pub partition_id: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct MorselRange {
    pub start: usize,
    pub end: usize,
}

pub struct MorselGenerator {
    morsel_size: usize,
    current_offset: AtomicUsize,
    total_rows: usize,
}

impl MorselGenerator {
    pub fn new(total_rows: usize, morsel_size: usize) -> Self {
        Self { morsel_size, current_offset: AtomicUsize::new(0), total_rows }
    }

    pub fn next_morsel(&self) -> Option<MorselRange> {
        let start = self.current_offset.fetch_add(self.morsel_size, Ordering::SeqCst);
        if start >= self.total_rows {
            return None;
        }
        let end = (start + self.morsel_size).min(self.total_rows);
        Some(MorselRange { start, end })
    }

    pub fn reset(&self) {
        self.current_offset.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_morsel_generation() {
        let gen = MorselGenerator::new(100, 10);
        let m1 = gen.next_morsel().unwrap();
        assert_eq!(m1.start, 0);
        assert_eq!(m1.end, 10);

        let m2 = gen.next_morsel().unwrap();
        assert_eq!(m2.start, 10);
        assert_eq!(m2.end, 20);
    }

    #[test]
    fn test_morsel_boundary() {
        let gen = MorselGenerator::new(95, 10);
        for _ in 0..9 {
            gen.next_morsel();
        }
        let last = gen.next_morsel().unwrap();
        assert_eq!(last.start, 90);
        assert_eq!(last.end, 95);
        assert!(gen.next_morsel().is_none());
    }

    #[test]
    fn test_empty_table() {
        let gen = MorselGenerator::new(0, 10);
        assert!(gen.next_morsel().is_none());
    }

    #[test]
    fn test_single_tuple() {
        let gen = MorselGenerator::new(1, 10);
        let m = gen.next_morsel().unwrap();
        assert_eq!(m.start, 0);
        assert_eq!(m.end, 1);
        assert!(gen.next_morsel().is_none());
    }
}
