use super::error::Result;

#[derive(Debug, Clone)]
pub struct Histogram {
    buckets: Vec<Bucket>,
    num_buckets: usize,
}

#[derive(Debug, Clone)]
struct Bucket {
    lower: i64,
    upper: i64,
    count: u64,
}

impl Histogram {
    pub fn new(num_buckets: usize) -> Self {
        Self {
            buckets: Vec::new(),
            num_buckets,
        }
    }
    
    pub fn build(&mut self, mut values: Vec<i64>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        
        values.sort_unstable();
        let total = values.len();
        let per_bucket = (total + self.num_buckets - 1) / self.num_buckets;
        
        self.buckets.clear();
        for chunk in values.chunks(per_bucket) {
            if !chunk.is_empty() {
                self.buckets.push(Bucket {
                    lower: chunk[0],
                    upper: chunk[chunk.len() - 1],
                    count: chunk.len() as u64,
                });
            }
        }
        
        Ok(())
    }
    
    pub fn estimate_selectivity(&self, value: i64) -> f64 {
        if self.buckets.is_empty() {
            return 0.0;
        }
        
        let total: u64 = self.buckets.iter().map(|b| b.count).sum();
        if total == 0 {
            return 0.0;
        }
        
        for bucket in &self.buckets {
            if value >= bucket.lower && value <= bucket.upper {
                return bucket.count as f64 / total as f64;
            }
        }
        
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_histogram_build() {
        let mut hist = Histogram::new(10);
        let values: Vec<i64> = (0..100).collect();
        hist.build(values).unwrap();
        assert!(!hist.buckets.is_empty());
    }
    
    #[test]
    fn test_histogram_selectivity() {
        let mut hist = Histogram::new(10);
        let values: Vec<i64> = (0..100).collect();
        hist.build(values).unwrap();
        
        let sel = hist.estimate_selectivity(50);
        assert!(sel > 0.0);
    }
}
