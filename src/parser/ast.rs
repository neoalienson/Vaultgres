/// SQL Statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Describe(DescribeStmt),
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

/// DESCRIBE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeStmt {
    pub table: String,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Data type
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Text,
    Varchar(u32),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct: bool,
    pub columns: Vec<Expr>,
    pub from: String,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<String>>,
    pub having: Option<Expr>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// ORDER BY expression
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub column: String,
    pub ascending: bool,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table: String,
    pub values: Vec<Expr>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
}

/// Expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Number(i64),
    String(String),
    Star,
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    Aggregate {
        func: AggregateFunc,
        arg: Box<Expr>,
    },
    List(Vec<Expr>),
}

/// Aggregate functions
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Like,
    In,
    Between,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_stmt_creation() {
        let stmt = SelectStmt {
            distinct: false,
            columns: vec![Expr::Star],
            from: "users".to_string(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        
        assert_eq!(stmt.from, "users");
        assert_eq!(stmt.columns.len(), 1);
    }
    
    #[test]
    fn test_insert_stmt_creation() {
        let stmt = InsertStmt {
            table: "users".to_string(),
            values: vec![Expr::Number(1), Expr::String("Alice".to_string())],
        };
        
        assert_eq!(stmt.table, "users");
        assert_eq!(stmt.values.len(), 2);
    }
}
