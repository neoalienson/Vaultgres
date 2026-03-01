/// SQL Statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    CreateMaterializedView(CreateMaterializedViewStmt),
    RefreshMaterializedView(RefreshMaterializedViewStmt),
    DropMaterializedView(DropMaterializedViewStmt),
    CreateTrigger(CreateTriggerStmt),
    DropTrigger(DropTriggerStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    Describe(DescribeStmt),
    Union(UnionStmt),
    Intersect(IntersectStmt),
    Except(ExceptStmt),
    With(WithStmt),
}

/// UNION statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UnionStmt {
    pub left: Box<SelectStmt>,
    pub right: Box<SelectStmt>,
    pub all: bool,
}

/// INTERSECT statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IntersectStmt {
    pub left: Box<SelectStmt>,
    pub right: Box<SelectStmt>,
}

/// EXCEPT statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExceptStmt {
    pub left: Box<SelectStmt>,
    pub right: Box<SelectStmt>,
}

/// WITH (CTE) statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WithStmt {
    pub ctes: Vec<CTE>,
    pub query: Box<SelectStmt>,
}

/// Common Table Expression
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CTE {
    pub name: String,
    pub query: Box<SelectStmt>,
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateViewStmt {
    pub name: String,
    pub query: Box<SelectStmt>,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropViewStmt {
    pub name: String,
    pub if_exists: bool,
}

/// CREATE MATERIALIZED VIEW statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateMaterializedViewStmt {
    pub name: String,
    pub query: Box<SelectStmt>,
}

/// REFRESH MATERIALIZED VIEW statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RefreshMaterializedViewStmt {
    pub name: String,
}

/// DROP MATERIALIZED VIEW statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropMaterializedViewStmt {
    pub name: String,
    pub if_exists: bool,
}

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateTriggerStmt {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub for_each: TriggerFor,
    pub when: Option<Expr>,
    pub body: Vec<Statement>,
}

/// DROP TRIGGER statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropTriggerStmt {
    pub name: String,
    pub if_exists: bool,
}

/// Trigger timing
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
}

/// Trigger event
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

/// Trigger granularity
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TriggerFor {
    EachRow,
    EachStatement,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateIndexStmt {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropIndexStmt {
    pub name: String,
    pub if_exists: bool,
}

/// DESCRIBE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DescribeStmt {
    pub table: String,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Data type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Int,
    Text,
    Varchar(u32),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectStmt {
    pub distinct: bool,
    pub columns: Vec<Expr>,
    pub from: String,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<String>>,
    pub having: Option<Expr>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// JOIN clause
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub on: Expr,
}

/// JOIN type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// ORDER BY expression
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderByExpr {
    pub column: String,
    pub ascending: bool,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InsertStmt {
    pub table: String,
    pub values: Vec<Expr>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
}

/// Expression
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Aggregate {
        func: AggregateFunc,
        arg: Box<Expr>,
    },
    Window {
        func: WindowFunc,
        arg: Box<Expr>,
        partition_by: Vec<String>,
        order_by: Vec<OrderByExpr>,
    },
    List(Vec<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Subquery(Box<SelectStmt>),
    Case {
        conditions: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
}

/// Aggregate functions
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Window functions
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
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

/// Unary operator
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
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
            joins: vec![],
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
