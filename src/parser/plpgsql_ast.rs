use crate::parser::ast::Expr;

#[derive(Debug, Clone)]
pub enum PlPgSqlStmt {
    Declare {
        name: String,
        data_type: String,
        default: Option<Expr>,
    },
    Assign {
        target: String,
        value: Expr,
    },
    If {
        condition: Expr,
        then_stmts: Vec<PlPgSqlStmt>,
        else_stmts: Vec<PlPgSqlStmt>,
    },
    While {
        condition: Expr,
        body: Vec<PlPgSqlStmt>,
    },
    For {
        var: String,
        start: Expr,
        end: Expr,
        body: Vec<PlPgSqlStmt>,
    },
    ForEach {
        var: String,
        array: Expr,
        body: Vec<PlPgSqlStmt>,
    },
    ForQuery {
        var: String,
        query: String,
        body: Vec<PlPgSqlStmt>,
    },
    Loop {
        body: Vec<PlPgSqlStmt>,
    },
    Exit,
    Continue,
    Case {
        expr: Expr,
        when_clauses: Vec<(Expr, Vec<PlPgSqlStmt>)>,
        else_stmts: Vec<PlPgSqlStmt>,
    },
    Return {
        value: Option<Expr>,
    },
    Execute {
        query: String,
    },
    Perform {
        query: String,
    },
    ExceptionBlock {
        try_stmts: Vec<PlPgSqlStmt>,
        exception_var: String,
        catch_stmts: Vec<PlPgSqlStmt>,
    },
    Raise {
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct PlPgSqlFunction {
    pub declarations: Vec<PlPgSqlStmt>,
    pub body: Vec<PlPgSqlStmt>,
}
