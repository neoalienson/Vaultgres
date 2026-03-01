use crate::executor::parallel::hash_agg::AggregateState;
use crate::executor::parallel::morsel::MorselGenerator;
use crate::executor::parallel::partition::PartitionStrategy;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_morsel_large_table() {
        let gen = MorselGenerator::new(1_000_000, 10_000);
        let mut count = 0;
        while gen.next_morsel().is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
    }

    #[test]
    fn test_morsel_odd_size() {
        let gen = MorselGenerator::new(1001, 100);
        let mut total = 0;
        while let Some(range) = gen.next_morsel() {
            total += range.end - range.start;
        }
        assert_eq!(total, 1001);
    }

    #[test]
    fn test_partition_single() {
        let strategy = PartitionStrategy::new(1);
        assert_eq!(strategy.partition_key(b"any_key"), 0);
    }

    #[test]
    fn test_partition_distribution() {
        let strategy = PartitionStrategy::new(8);
        let mut counts = vec![0; 8];

        for i in 0..1000 {
            let key = format!("key_{}", i);
            let partition = strategy.partition_key(key.as_bytes());
            counts[partition] += 1;
        }

        // Check reasonable distribution (no partition should be empty or have > 50%)
        for count in counts {
            assert!(count > 0);
            assert!(count < 500);
        }
    }

    #[test]
    fn test_aggregate_empty() {
        let state = AggregateState::new();
        assert_eq!(state.count, 0);
        assert_eq!(state.sum, 0.0);
        assert!(state.min.is_none());
        assert!(state.max.is_none());
    }

    #[test]
    fn test_aggregate_single_value() {
        let mut state = AggregateState::new();
        state.update(&[42]);
        assert_eq!(state.count, 1);
        assert_eq!(state.sum, 42.0);
        assert_eq!(state.min, Some(vec![42]));
        assert_eq!(state.max, Some(vec![42]));
    }

    #[test]
    fn test_aggregate_merge_empty() {
        let mut state1 = AggregateState::new();
        let state2 = AggregateState::new();
        state1.merge(&state2);
        assert_eq!(state1.count, 0);
    }

    #[test]
    fn test_morsel_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let gen = Arc::new(MorselGenerator::new(1000, 10));
        let mut handles = vec![];

        for _ in 0..4 {
            let gen_clone = Arc::clone(&gen);
            let handle = thread::spawn(move || {
                let mut count = 0;
                while gen_clone.next_morsel().is_some() {
                    count += 1;
                }
                count
            });
            handles.push(handle);
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_partition_max_partitions() {
        let strategy = PartitionStrategy::new(1024);
        assert_eq!(strategy.num_partitions(), 1024);

        let partition = strategy.partition_key(b"test");
        assert!(partition < 1024);
    }

    #[test]
    fn test_morsel_reset() {
        let gen = MorselGenerator::new(100, 10);

        for _ in 0..5 {
            gen.next_morsel();
        }

        gen.reset();
        let first = gen.next_morsel().unwrap();
        assert_eq!(first.start, 0);
        assert_eq!(first.end, 10);
    }

    #[test]
    fn test_aggregate_extreme_values() {
        let mut state = AggregateState::new();
        state.update(&[0]);
        state.update(&[255]);

        assert_eq!(state.count, 2);
        assert_eq!(state.min, Some(vec![0]));
        assert_eq!(state.max, Some(vec![255]));
    }

    #[test]
    fn test_partition_empty_key() {
        let strategy = PartitionStrategy::new(4);
        let partition = strategy.partition_key(&[]);
        assert!(partition < 4);
    }

    #[test]
    fn test_morsel_single_large_morsel() {
        let gen = MorselGenerator::new(100, 1000);
        let morsel = gen.next_morsel().unwrap();
        assert_eq!(morsel.start, 0);
        assert_eq!(morsel.end, 100);
        assert!(gen.next_morsel().is_none());
    }
}
