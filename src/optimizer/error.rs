use std::fmt;

pub type Result<T> = std::result::Result<T, OptimizerError>;

#[derive(Debug)]
pub enum OptimizerError {
    InvalidCost,
    NoStatistics,
}

impl fmt::Display for OptimizerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidCost => write!(f, "Invalid cost value"),
            Self::NoStatistics => write!(f, "Statistics not available"),
        }
    }
}

impl std::error::Error for OptimizerError {}
