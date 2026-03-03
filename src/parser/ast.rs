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
    CreateFunction(CreateFunctionStmt),
    DropFunction(DropFunctionStmt),
    DeclareCursor(DeclareCursorStmt),
    FetchCursor(FetchCursorStmt),
    CloseCursor(CloseCursorStmt),
    Begin,
    Commit,
    Rollback,
    SetTransaction(IsolationLevel),
    Savepoint(String),
    RollbackTo(String),
    ReleaseSavepoint(String),
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(String),
}

/// Isolation level
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// PREPARE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PrepareStmt {
    pub name: String,
    pub statement: Box<Statement>,
}

/// EXECUTE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExecuteStmt {
    pub name: String,
    pub params: Vec<Expr>,
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
    pub recursive: bool,
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
    pub expressions: Vec<Expr>,
    pub unique: bool,
    pub where_clause: Option<Expr>,
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

/// CREATE FUNCTION statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateFunctionStmt {
    pub name: String,
    pub parameters: Vec<FunctionParameter>,
    pub return_type: FunctionReturnType,
    pub language: String,
    pub body: String,
    pub volatility: Option<FunctionVolatility>,
    pub cost: Option<f64>,
    pub rows: Option<u64>,
}

/// Function volatility
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}

/// Function parameter
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FunctionParameter {
    pub name: String,
    pub data_type: String,
    pub mode: ParameterMode,
    pub default: Option<String>,
}

/// Parameter mode
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Variadic,
}

/// Function return type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FunctionReturnType {
    Type(String),
    Table(Vec<(String, String)>),
    Setof(String),
}

/// DROP FUNCTION statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropFunctionStmt {
    pub name: String,
    pub if_exists: bool,
}

/// DECLARE CURSOR statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeclareCursorStmt {
    pub name: String,
    pub query: Box<SelectStmt>,
}

/// FETCH CURSOR statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FetchCursorStmt {
    pub name: String,
    pub direction: FetchDirection,
    pub count: Option<i64>,
}

/// Fetch direction
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute,
    Relative,
    Forward,
    Backward,
}

/// CLOSE CURSOR statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CloseCursorStmt {
    pub name: String,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub check_constraints: Vec<CheckConstraint>,
    pub unique_constraints: Vec<UniqueConstraint>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_auto_increment: bool,
    pub is_not_null: bool,
    pub default_value: Option<Expr>,
    pub foreign_key: Option<ForeignKeyRef>,
}

impl ColumnDef {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        }
    }
}

/// Foreign key reference (column-level)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
}

/// Foreign key action
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ForeignKeyAction {
    Cascade,
    SetNull,
    Restrict,
}

/// Foreign key definition (table-level)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForeignKeyDef {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

/// CHECK constraint definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expr: Expr,
}

/// UNIQUE constraint definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UniqueConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

/// Data type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Int,
    Serial,
    Text,
    Varchar(u32),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectStmt {
    pub distinct: bool,
    pub columns: Vec<Expr>,
    pub from: String,
    pub table_alias: Option<String>,
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
    pub lateral: bool,
    pub table: String,
    pub alias: Option<String>,
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
    Parameter(usize),
    Alias {
        expr: Box<Expr>,
        alias: String,
    },
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
    ILike,
    In,
    Any,
    All,
    Some,
    Between,
    Add,
    StringConcat,
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
            table_alias: None,
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
