use crate::executor::executor::{ExecutorError, SimpleTuple};
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use std::sync::Arc;

pub struct ParallelSort {
    child: Arc<dyn ParallelOperator>,
    ascending: bool,
}

impl ParallelSort {
    pub fn new(child: Arc<dyn ParallelOperator>, ascending: bool) -> Self {
        Self { child, ascending }
    }

    pub fn local_sort(&self, morsel: Morsel) -> Result<Vec<SimpleTuple>, ExecutorError> {
        let result = self.child.process_morsel(morsel)?;
        let mut tuples = result.tuples;

        tuples.sort_by(
            |a, b| {
                if self.ascending {
                    a.data.cmp(&b.data)
                } else {
                    b.data.cmp(&a.data)
                }
            },
        );

        Ok(tuples)
    }

    pub fn merge_sorted_runs(&self, mut runs: Vec<Vec<SimpleTuple>>) -> Vec<SimpleTuple> {
        if runs.is_empty() {
            return vec![];
        }

        if runs.len() == 1 {
            return runs.pop().unwrap();
        }

        let mut result = Vec::new();
        let mut indices = vec![0; runs.len()];

        loop {
            let mut min_idx = None;
            let mut min_val = None;

            for (run_idx, run) in runs.iter().enumerate() {
                if indices[run_idx] < run.len() {
                    let val = &run[indices[run_idx]];
                    if min_val.is_none() || Some(&val.data) < min_val {
                        min_val = Some(&val.data);
                        min_idx = Some(run_idx);
                    }
                }
            }

            match min_idx {
                Some(idx) => {
                    result.push(runs[idx][indices[idx]].clone());
                    indices[idx] += 1;
                }
                None => break,
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockOperator {
        tuples: Vec<SimpleTuple>,
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    #[test]
    fn test_local_sort() {
        let tuples = vec![
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
        ];

        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(child, true);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        let result = sort.local_sort(morsel).unwrap();
        assert_eq!(result[0].data, vec![1]);
        assert_eq!(result[1].data, vec![2]);
        assert_eq!(result[2].data, vec![3]);
    }

    #[test]
    fn test_merge_sorted_runs() {
        let run1 = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![3] }];
        let run2 = vec![SimpleTuple { data: vec![2] }, SimpleTuple { data: vec![4] }];

        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(child, true);

        let result = sort.merge_sorted_runs(vec![run1, run2]);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].data, vec![1]);
        assert_eq!(result[1].data, vec![2]);
        assert_eq!(result[2].data, vec![3]);
        assert_eq!(result[3].data, vec![4]);
    }

    #[test]
    fn test_empty_merge() {
        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(child, true);

        let result = sort.merge_sorted_runs(vec![]);
        assert_eq!(result.len(), 0);
    }
}
