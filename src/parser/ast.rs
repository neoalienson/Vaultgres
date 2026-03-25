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
    CreateAggregate(CreateAggregateStmt),
    DropAggregate(DropAggregateStmt),
    CreateType(CreateTypeStmt),
    DropType(DropTypeStmt),
    AlterType(AlterTypeStmt),
    AttachPartition(AttachPartitionStmt),
    DetachPartition(DetachPartitionStmt),
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

/// CREATE AGGREGATE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateAggregateStmt {
    pub name: String,
    pub input_type: String,
    pub sfunc: String,
    pub stype: String,
    pub finalfunc: Option<String>,
    pub initcond: Option<String>,
    pub volatility: Option<FunctionVolatility>,
    pub cost: Option<f64>,
}

/// DROP AGGREGATE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropAggregateStmt {
    pub name: String,
    pub if_exists: bool,
}

/// Enum type definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EnumTypeDef {
    pub type_name: String,
    pub labels: Vec<String>,
}

/// Composite type definition
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompositeTypeDef {
    pub type_name: String,
    pub fields: Vec<(String, DataType)>,
}

/// Kind of type (enum or composite)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TypeKind {
    Enum(Vec<String>),
    Composite(Vec<ColumnDef>),
}

/// CREATE TYPE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CreateTypeStmt {
    pub type_name: String,
    pub kind: TypeKind,
}

/// DROP TYPE statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DropTypeStmt {
    pub type_name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// ALTER TYPE statement (for adding values to enum)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AlterTypeStmt {
    pub type_name: String,
    pub new_label: String,
    pub after_label: Option<String>,
}

/// Partition method (RANGE, LIST, HASH)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionMethod {
    Range,
    List,
    Hash,
}

/// Partition key specification
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionKey {
    pub column: String,
    pub opclass: Option<String>,
}

/// FOR VALUES FROM ... TO ... (for RANGE partitions)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionRangeBound {
    pub from_values: Vec<Expr>,
    pub to_values: Vec<Expr>,
}

/// FOR VALUES IN ... (for LIST partitions)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionListBound {
    pub values: Vec<Expr>,
}

/// FOR VALUES WITH (MODULUS n, REMAINDER r) (for HASH partitions)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionHashBound {
    pub modulus: u64,
    pub remainder: u64,
}

/// Partition bound specification
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionBoundSpec {
    Range(PartitionRangeBound),
    List(PartitionListBound),
    Hash(PartitionHashBound),
    Default,
}

/// Partition definition (for a child partition)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionDef {
    pub name: String,
    pub bound: PartitionBoundSpec,
}

/// ATTACH PARTITION statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AttachPartitionStmt {
    pub parent_table: String,
    pub partition_name: String,
    pub bound: PartitionBoundSpec,
}

/// DETACH PARTITION statement
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DetachPartitionStmt {
    pub parent_table: String,
    pub partition_name: String,
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
    pub partition_by: Option<(PartitionMethod, Vec<PartitionKey>)>,
    pub partitions: Vec<PartitionDef>,
    pub is_partition: bool,
    pub parent_table: Option<String>,
    pub partition_bound: Option<PartitionBoundSpec>,
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
    Float,
    Text,
    Varchar(u32),
    Boolean,
    Date,
    Time,
    Timestamp,
    Decimal(u8, u8),
    Bytea,
    Json,
    Jsonb,
    Enum(String),
    Array(Box<DataType>),
    Composite(String),
    Int4Range,
    Int8Range,
    NumRange,
    DateRange,
    TsRange,
    TsTzRange,
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
    pub group_by: Option<Vec<Expr>>,
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
    pub columns: Vec<String>,
    pub values: Vec<Expr>,
    pub batch_values: Vec<Vec<Expr>>,
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
    QualifiedColumn {
        table: String,
        column: String,
    },
    Number(i64),
    Float(f64),
    String(String),
    Null,
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
    CustomAggregate {
        name: String,
        arg: Box<Expr>,
    },
    Window {
        func: WindowFunc,
        arg: Box<Expr>,
        partition_by: Vec<String>,
        order_by: Vec<OrderByExpr>,
        window_frame: Option<WindowFrame>,
    },
    List(Vec<Expr>),
    Array(Vec<Expr>),
    Range {
        lower: Box<Expr>,
        lower_inclusive: bool,
        upper: Box<Expr>,
        upper_inclusive: bool,
    },
    Row(Vec<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Subquery(Box<SelectStmt>),
    Case {
        conditions: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
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
    DenseRankWithNulls,
    PercentRank,
    CumeDist,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Ntile,
}

/// Window frame type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameMode {
    Rows,
    Range,
    Groups,
}

/// Window frame bound
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    UnboundedFollowing,
    CurrentRow,
    Preceding(i64),
    Following(i64),
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
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
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StringConcat,
    JsonExtract,
    JsonExtractText,
    JsonPath,
    JsonPathText,
    JsonExists,
    JsonExistsAny,
    JsonExistsAll,
    ArrayContains,
    ArrayContainedBy,
    ArrayOverlaps,
    ArrayConcat,
    ArrayAccess,
    RangeContains,
    RangeContainedBy,
    RangeOverlaps,
    RangeLeftOf,
    RangeRightOf,
    RangeAdjacent,
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
            columns: Vec::new(),
            values: vec![Expr::Number(1), Expr::String("Alice".to_string())],
            batch_values: vec![],
        };

        assert_eq!(stmt.table, "users");
        assert_eq!(stmt.values.len(), 2);
    }

    #[test]
    fn test_isolation_level_creation() {
        assert_eq!(IsolationLevel::ReadCommitted, IsolationLevel::ReadCommitted);
        assert_eq!(IsolationLevel::RepeatableRead, IsolationLevel::RepeatableRead);
        assert_eq!(IsolationLevel::Serializable, IsolationLevel::Serializable);
    }

    #[test]
    fn test_prepare_stmt_creation() {
        let prepare_stmt = PrepareStmt {
            name: "my_query".to_string(),
            statement: Box::new(Statement::Select(SelectStmt {
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
            })),
        };
        assert_eq!(prepare_stmt.name, "my_query");
        assert!(matches!(*prepare_stmt.statement, Statement::Select(_)));
    }

    #[test]
    fn test_execute_stmt_creation() {
        let execute_stmt = ExecuteStmt {
            name: "my_query".to_string(),
            params: vec![Expr::Number(1), Expr::String("test".to_string())],
        };
        assert_eq!(execute_stmt.name, "my_query");
        assert_eq!(execute_stmt.params.len(), 2);
    }

    #[test]
    fn test_statement_begin_commit_rollback() {
        assert!(matches!(Statement::Begin, Statement::Begin));
        assert!(matches!(Statement::Commit, Statement::Commit));
        assert!(matches!(Statement::Rollback, Statement::Rollback));
    }

    #[test]
    fn test_statement_set_transaction() {
        let stmt = Statement::SetTransaction(IsolationLevel::Serializable);
        if let Statement::SetTransaction(level) = stmt {
            assert_eq!(level, IsolationLevel::Serializable);
        } else {
            panic!("Expected SetTransaction statement");
        }
    }

    #[test]
    fn test_statement_savepoint() {
        let stmt = Statement::Savepoint("my_savepoint".to_string());
        if let Statement::Savepoint(name) = stmt {
            assert_eq!(name, "my_savepoint");
        } else {
            panic!("Expected Savepoint statement");
        }
    }

    #[test]
    fn test_statement_rollback_to() {
        let stmt = Statement::RollbackTo("my_savepoint".to_string());
        if let Statement::RollbackTo(name) = stmt {
            assert_eq!(name, "my_savepoint");
        } else {
            panic!("Expected RollbackTo statement");
        }
    }

    #[test]
    fn test_statement_release_savepoint() {
        let stmt = Statement::ReleaseSavepoint("my_savepoint".to_string());
        if let Statement::ReleaseSavepoint(name) = stmt {
            assert_eq!(name, "my_savepoint");
        } else {
            panic!("Expected ReleaseSavepoint statement");
        }
    }

    #[test]
    fn test_statement_prepare_execute_deallocate() {
        let prepare_stmt =
            PrepareStmt { name: "q1".to_string(), statement: Box::new(Statement::Begin) };
        assert!(matches!(Statement::Prepare(prepare_stmt), Statement::Prepare(_)));

        let execute_stmt = ExecuteStmt { name: "q1".to_string(), params: vec![] };
        assert!(matches!(Statement::Execute(execute_stmt), Statement::Execute(_)));

        assert!(matches!(Statement::Deallocate("q1".to_string()), Statement::Deallocate(_)));
    }

    // Helper for creating a blank SelectStmt for testing compound statements
    fn blank_select() -> Box<SelectStmt> {
        Box::new(SelectStmt {
            distinct: false,
            columns: vec![Expr::Star],
            from: "dummy".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })
    }

    #[test]
    fn test_union_stmt_creation() {
        let union_stmt = UnionStmt { left: blank_select(), right: blank_select(), all: true };
        assert!(union_stmt.all);
        assert!(matches!(*union_stmt.left, SelectStmt { .. }));
    }

    #[test]
    fn test_intersect_stmt_creation() {
        let intersect_stmt = IntersectStmt { left: blank_select(), right: blank_select() };
        assert!(matches!(*intersect_stmt.left, SelectStmt { .. }));
    }

    #[test]
    fn test_except_stmt_creation() {
        let except_stmt = ExceptStmt { left: blank_select(), right: blank_select() };
        assert!(matches!(*except_stmt.left, SelectStmt { .. }));
    }

    #[test]
    fn test_with_stmt_creation() {
        let with_stmt = WithStmt {
            recursive: true,
            ctes: vec![CTE { name: "cte1".to_string(), query: blank_select() }],
            query: blank_select(),
        };
        assert!(with_stmt.recursive);
        assert_eq!(with_stmt.ctes.len(), 1);
        assert_eq!(with_stmt.ctes[0].name, "cte1");
    }

    #[test]
    fn test_ddl_structs_creation() {
        // DropTableStmt
        let drop_table = DropTableStmt { table: "users".to_string(), if_exists: true };
        assert_eq!(drop_table.table, "users");
        assert!(drop_table.if_exists);

        // CreateViewStmt
        let create_view = CreateViewStmt { name: "user_view".to_string(), query: blank_select() };
        assert_eq!(create_view.name, "user_view");
        assert!(matches!(*create_view.query, SelectStmt { .. }));

        // DropViewStmt
        let drop_view = DropViewStmt { name: "user_view".to_string(), if_exists: false };
        assert_eq!(drop_view.name, "user_view");
        assert!(!drop_view.if_exists);

        // CreateMaterializedViewStmt
        let create_mview =
            CreateMaterializedViewStmt { name: "user_mview".to_string(), query: blank_select() };
        assert_eq!(create_mview.name, "user_mview");
        assert!(matches!(*create_mview.query, SelectStmt { .. }));

        // RefreshMaterializedViewStmt
        let refresh_mview = RefreshMaterializedViewStmt { name: "user_mview".to_string() };
        assert_eq!(refresh_mview.name, "user_mview");

        // DropMaterializedViewStmt
        let drop_mview =
            DropMaterializedViewStmt { name: "user_mview".to_string(), if_exists: true };
        assert_eq!(drop_mview.name, "user_mview");
        assert!(drop_mview.if_exists);

        // CreateTriggerStmt
        let create_trigger = CreateTriggerStmt {
            name: "my_trigger".to_string(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            table: "users".to_string(),
            for_each: TriggerFor::EachRow,
            when: None,
            body: vec![],
        };
        assert_eq!(create_trigger.name, "my_trigger");
        assert_eq!(create_trigger.timing, TriggerTiming::After);

        // DropTriggerStmt
        let drop_trigger = DropTriggerStmt { name: "my_trigger".to_string(), if_exists: false };
        assert_eq!(drop_trigger.name, "my_trigger");

        // CreateIndexStmt
        let create_index = CreateIndexStmt {
            name: "my_index".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            expressions: vec![],
            unique: true,
            where_clause: None,
        };
        assert_eq!(create_index.name, "my_index");
        assert!(create_index.unique);

        // DropIndexStmt
        let drop_index = DropIndexStmt { name: "my_index".to_string(), if_exists: true };
        assert_eq!(drop_index.name, "my_index");
        assert!(drop_index.if_exists);

        // DescribeStmt
        let describe = DescribeStmt { table: "users".to_string() };
        assert_eq!(describe.table, "users");
    }

    #[test]
    fn test_function_and_cursor_structs_creation() {
        // CreateFunctionStmt
        let create_function = CreateFunctionStmt {
            name: "add".to_string(),
            parameters: vec![FunctionParameter {
                name: "a".to_string(),
                data_type: "INTEGER".to_string(),
                mode: ParameterMode::In,
                default: None,
            }],
            return_type: FunctionReturnType::Type("INTEGER".to_string()),
            language: "sql".to_string(),
            body: "$1 + $2".to_string(),
            volatility: Some(FunctionVolatility::Immutable),
            cost: Some(1.0),
            rows: Some(1),
        };
        assert_eq!(create_function.name, "add");
        assert_eq!(create_function.parameters.len(), 1);

        // DropFunctionStmt
        let drop_function = DropFunctionStmt { name: "add".to_string(), if_exists: true };
        assert_eq!(drop_function.name, "add");
        assert!(drop_function.if_exists);

        // DeclareCursorStmt
        let declare_cursor =
            DeclareCursorStmt { name: "my_cursor".to_string(), query: blank_select() };
        assert_eq!(declare_cursor.name, "my_cursor");

        // FetchCursorStmt
        let fetch_cursor = FetchCursorStmt {
            name: "my_cursor".to_string(),
            direction: FetchDirection::Next,
            count: Some(10),
        };
        assert_eq!(fetch_cursor.name, "my_cursor");
        assert_eq!(fetch_cursor.direction, FetchDirection::Next);

        // CloseCursorStmt
        let close_cursor = CloseCursorStmt { name: "my_cursor".to_string() };
        assert_eq!(close_cursor.name, "my_cursor");
    }

    #[test]
    fn test_table_and_constraint_structs_creation() {
        // ColumnDef
        let col_def = ColumnDef::new("id".to_string(), DataType::Int);
        assert_eq!(col_def.name, "id");
        assert_eq!(col_def.data_type, DataType::Int);

        // ForeignKeyRef
        let fk_ref =
            ForeignKeyRef { table: "other_table".to_string(), column: "other_id".to_string() };
        assert_eq!(fk_ref.table, "other_table");

        // ForeignKeyDef
        let fk_def = ForeignKeyDef {
            columns: vec!["fk_id".to_string()],
            ref_table: "ref_table".to_string(),
            ref_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::SetNull,
        };
        assert_eq!(fk_def.columns.len(), 1);
        assert_eq!(fk_def.on_delete, ForeignKeyAction::Cascade);

        // CheckConstraint
        let check_constraint = CheckConstraint {
            name: Some("age_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: crate::parser::ast::BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(18)),
            },
        };
        assert_eq!(check_constraint.name.clone().unwrap(), "age_check");

        // UniqueConstraint
        let unique_constraint = UniqueConstraint { name: None, columns: vec!["email".to_string()] };
        assert_eq!(unique_constraint.columns[0], "email");

        // CreateTableStmt
        let create_table = CreateTableStmt {
            table: "new_table".to_string(),
            columns: vec![col_def],
            primary_key: Some(vec!["id".to_string()]),
            foreign_keys: vec![fk_def],
            check_constraints: vec![check_constraint],
            unique_constraints: vec![unique_constraint],
            partition_by: None,
            partitions: vec![],
            is_partition: false,
            parent_table: None,
            partition_bound: None,
        };
        assert_eq!(create_table.table, "new_table");
        assert_eq!(create_table.columns.len(), 1);
        assert_eq!(create_table.foreign_keys.len(), 1);
    }

    #[test]
    fn test_data_type_creation() {
        assert_eq!(DataType::Int, DataType::Int);
        assert_eq!(DataType::Serial, DataType::Serial);
        assert_eq!(DataType::Text, DataType::Text);
        assert_eq!(DataType::Varchar(100), DataType::Varchar(100));
        assert_eq!(DataType::Boolean, DataType::Boolean);
        assert_eq!(DataType::Date, DataType::Date);
        assert_eq!(DataType::Time, DataType::Time);
        assert_eq!(DataType::Timestamp, DataType::Timestamp);
        assert_eq!(DataType::Decimal(10, 2), DataType::Decimal(10, 2));
        assert_eq!(DataType::Bytea, DataType::Bytea);
    }

    #[test]
    fn test_join_and_order_by_structs_creation() {
        // JoinClause
        let join_clause = JoinClause {
            join_type: JoinType::Left,
            lateral: false,
            table: "orders".to_string(),
            alias: Some("o".to_string()),
            on: Expr::BinaryOp {
                left: Box::new(Expr::QualifiedColumn {
                    table: "users".to_string(),
                    column: "id".to_string(),
                }),
                op: crate::parser::ast::BinaryOperator::Equals,
                right: Box::new(Expr::QualifiedColumn {
                    table: "o".to_string(),
                    column: "user_id".to_string(),
                }),
            },
        };
        assert_eq!(join_clause.join_type, JoinType::Left);
        assert_eq!(join_clause.table, "orders");

        // OrderByExpr
        let order_by_expr = OrderByExpr { column: "age".to_string(), ascending: false };
        assert_eq!(order_by_expr.column, "age");
        assert!(!order_by_expr.ascending);
    }

    #[test]
    fn test_update_and_delete_stmts_creation() {
        // UpdateStmt
        let update_stmt = UpdateStmt {
            table: "users".to_string(),
            assignments: vec![("age".to_string(), Expr::Number(30))],
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: crate::parser::ast::BinaryOperator::Equals,
                right: Box::new(Expr::Number(1)),
            }),
        };
        assert_eq!(update_stmt.table, "users");
        assert_eq!(update_stmt.assignments.len(), 1);

        // DeleteStmt
        let delete_stmt = DeleteStmt { table: "users".to_string(), where_clause: None };
        assert_eq!(delete_stmt.table, "users");
        assert!(delete_stmt.where_clause.is_none());
    }

    #[test]
    fn test_simple_expr_variants_creation() {
        assert_eq!(Expr::Column("id".to_string()), Expr::Column("id".to_string()));
        assert_eq!(
            Expr::QualifiedColumn { table: "users".to_string(), column: "id".to_string() },
            Expr::QualifiedColumn { table: "users".to_string(), column: "id".to_string() }
        );
        assert_eq!(Expr::Number(123), Expr::Number(123));
        assert_eq!(Expr::String("test".to_string()), Expr::String("test".to_string()));
        assert_eq!(Expr::Star, Expr::Star);
        assert_eq!(Expr::Parameter(1), Expr::Parameter(1));
        assert_eq!(
            Expr::Alias {
                expr: Box::new(Expr::Column("age".to_string())),
                alias: "user_age".to_string(),
            },
            Expr::Alias {
                expr: Box::new(Expr::Column("age".to_string())),
                alias: "user_age".to_string(),
            }
        );
    }

    #[test]
    fn test_complex_expr_variants_creation() {
        // BinaryOp
        let binary_op = Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(2)),
        };
        assert!(matches!(binary_op, Expr::BinaryOp { .. }));

        // UnaryOp
        let unary_op = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Column("is_active".to_string())),
        };
        assert!(matches!(unary_op, Expr::UnaryOp { .. }));

        // Aggregate
        let aggregate = Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) };
        assert!(matches!(aggregate, Expr::Aggregate { .. }));

        // Window
        let window = Expr::Window {
            func: WindowFunc::RowNumber,
            arg: Box::new(Expr::Star),
            partition_by: vec!["department".to_string()],
            order_by: vec![OrderByExpr { column: "salary".to_string(), ascending: false }],
            window_frame: None,
        };
        assert!(matches!(window, Expr::Window { .. }));

        // List
        let list = Expr::List(vec![Expr::Number(1), Expr::Number(2)]);
        assert!(matches!(list, Expr::List(_)));

        // IsNull / IsNotNull
        let is_null = Expr::IsNull(Box::new(Expr::Column("manager_id".to_string())));
        assert!(matches!(is_null, Expr::IsNull(_)));
        let is_not_null = Expr::IsNotNull(Box::new(Expr::Column("manager_id".to_string())));
        assert!(matches!(is_not_null, Expr::IsNotNull(_)));

        // Subquery
        let subquery = Expr::Subquery(blank_select());
        assert!(matches!(subquery, Expr::Subquery(_)));

        // Case
        let case = Expr::Case {
            conditions: vec![(Expr::String("active".to_string()), Expr::String("A".to_string()))],
            else_expr: Some(Box::new(Expr::String("I".to_string()))),
        };
        assert!(matches!(case, Expr::Case { .. }));

        // FunctionCall
        let function_call = Expr::FunctionCall {
            name: "LOWER".to_string(),
            args: vec![Expr::String("TEXT".to_string())],
        };
        assert!(matches!(function_call, Expr::FunctionCall { .. }));
    }
}
