use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

pub struct SubqueryScanExecutor {
    child: Box<dyn Executor>,
}

impl SubqueryScanExecutor {
    pub fn new(child: Box<dyn Executor>) -> Self {
        Self { child }
    }
}

impl Executor for SubqueryScanExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.child.next()
    }
}
