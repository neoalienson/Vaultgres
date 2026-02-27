use std::fmt;

pub type Result<T> = std::result::Result<T, StatisticsError>;

#[derive(Debug)]
pub enum StatisticsError {
    InvalidSampleSize,
    EmptyTable,
    InvalidHistogram,
}

impl fmt::Display for StatisticsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidSampleSize => write!(f, "Invalid sample size"),
            Self::EmptyTable => write!(f, "Cannot analyze empty table"),
            Self::InvalidHistogram => write!(f, "Invalid histogram configuration"),
        }
    }
}

impl std::error::Error for StatisticsError {}
