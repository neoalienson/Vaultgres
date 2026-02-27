use crate::parser::Expr;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan { table: String, filter: Option<Expr>, columns: Vec<String> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, columns: Vec<String> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, condition: Option<Expr> },
}

impl LogicalPlan {
    pub fn scan(table: String) -> Self {
        Self::Scan { table, filter: None, columns: vec![] }
    }
    
    pub fn filter(input: LogicalPlan, predicate: Expr) -> Self {
        Self::Filter { input: Box::new(input), predicate }
    }
    
    pub fn project(input: LogicalPlan, columns: Vec<String>) -> Self {
        Self::Project { input: Box::new(input), columns }
    }
    
    pub fn join(left: LogicalPlan, right: LogicalPlan) -> Self {
        Self::Join { left: Box::new(left), right: Box::new(right), condition: None }
    }
}
