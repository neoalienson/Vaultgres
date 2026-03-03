use std::time::Duration;

pub struct SlowQueryLogger {
    threshold: Duration,
}

impl SlowQueryLogger {
    pub fn new(threshold_ms: u64) -> Self {
        Self { threshold: Duration::from_millis(threshold_ms) }
    }

    pub fn log_if_slow(&self, query: &str, duration: Duration) {
        if duration >= self.threshold {
            log::warn!("SLOW QUERY ({:.2}ms): {}", duration.as_secs_f64() * 1000.0, query);
        }
    }

    pub fn set_threshold(&mut self, threshold_ms: u64) {
        self.threshold = Duration::from_millis(threshold_ms);
    }
}

impl Default for SlowQueryLogger {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threshold() {
        let logger = SlowQueryLogger::new(100);
        logger.log_if_slow("SELECT 1", Duration::from_millis(50));
        logger.log_if_slow("SELECT 2", Duration::from_millis(150));
    }

    #[test]
    fn test_set_threshold() {
        let mut logger = SlowQueryLogger::new(100);
        logger.set_threshold(200);
        assert_eq!(logger.threshold, Duration::from_millis(200));
    }
}
