use super::ast::*;
use super::error::{ParseError, Result};
use super::lexer::{Lexer, Token};

mod ddl;
mod dml;
mod expr;
mod select;

#[cfg(test)]
mod join_tests;

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    /// Creates a new parser
    pub fn new(sql: &str) -> Result<Self> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;

        Ok(Self { tokens, position: 0 })
    }

    /// Parses a SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = match self.current_token() {
            Token::With => self.parse_with(),
            Token::Declare => ddl::parse_declare_cursor(self),
            Token::Fetch => ddl::parse_fetch_cursor(self),
            Token::Close => ddl::parse_close_cursor(self),
            Token::Begin => {
                self.advance();
                Ok(Statement::Begin)
            }
            Token::Commit => {
                self.advance();
                Ok(Statement::Commit)
            }
            Token::Rollback => self.parse_rollback(),
            Token::Set => self.parse_set(),
            Token::Savepoint => {
                self.advance();
                Ok(Statement::Savepoint(self.expect_identifier()?))
            }
            Token::Release => self.parse_release(),
            Token::Prepare => self.parse_prepare(),
            Token::Execute => self.parse_execute(),
            Token::Deallocate => {
                self.advance();
                Ok(Statement::Deallocate(self.expect_identifier()?))
            }
            Token::Select => self.parse_select_with_set_ops(),
            Token::Refresh => self.parse_refresh(),
            Token::Insert => dml::parse_insert(self),
            Token::Update => dml::parse_update(self),
            Token::Delete => dml::parse_delete(self),
            Token::Create => ddl::parse_create(self),
            Token::Drop => ddl::parse_drop(self),
            Token::Describe => ddl::parse_describe(self),
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }?;

        if self.current_token() == &Token::Semicolon {
            self.advance();
        }
        Ok(stmt)
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        self.advance();
        if self.current_token() == &Token::To {
            self.advance();
            if self.current_token() == &Token::Savepoint {
                self.advance();
            }
            Ok(Statement::RollbackTo(self.expect_identifier()?))
        } else {
            Ok(Statement::Rollback)
        }
    }

    fn parse_set(&mut self) -> Result<Statement> {
        self.advance();
        if self.current_token() != &Token::Transaction {
            return Err(ParseError::InvalidSyntax("Expected TRANSACTION".to_string()));
        }
        self.advance();
        self.expect(Token::Isolation)?;
        self.expect(Token::Level)?;

        let level = match self.current_token() {
            Token::Read => {
                self.advance();
                self.expect(Token::Committed)?;
                IsolationLevel::ReadCommitted
            }
            Token::Repeatable => {
                self.advance();
                self.expect(Token::Read)?;
                IsolationLevel::RepeatableRead
            }
            Token::Serializable => {
                self.advance();
                IsolationLevel::Serializable
            }
            _ => return Err(ParseError::InvalidSyntax("Invalid isolation level".to_string())),
        };
        Ok(Statement::SetTransaction(level))
    }

    fn parse_release(&mut self) -> Result<Statement> {
        self.advance();
        if self.current_token() == &Token::Savepoint {
            self.advance();
        }
        Ok(Statement::ReleaseSavepoint(self.expect_identifier()?))
    }

    fn parse_select_with_set_ops(&mut self) -> Result<Statement> {
        let select = select::parse_select(self)?;
        match self.current_token() {
            Token::Union => self.parse_union(select),
            Token::Intersect => self.parse_intersect(select),
            Token::Except => self.parse_except(select),
            _ => Ok(select),
        }
    }

    fn parse_union(&mut self, left: Statement) -> Result<Statement> {
        use crate::parser::ast::UnionStmt;

        let left_select = match left {
            Statement::Select(s) => s,
            _ => return Err(ParseError::InvalidSyntax("UNION requires SELECT".to_string())),
        };

        self.expect(Token::Union)?;
        let all = if self.current_token() == &Token::All {
            self.advance();
            true
        } else {
            false
        };

        let right = select::parse_select(self)?;
        let right_select = match right {
            Statement::Select(s) => s,
            _ => return Err(ParseError::InvalidSyntax("UNION requires SELECT".to_string())),
        };

        Ok(Statement::Union(UnionStmt {
            left: Box::new(left_select),
            right: Box::new(right_select),
            all,
        }))
    }

    pub(crate) fn expect(&mut self, expected: Token) -> Result<()> {
        if self.current_token() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token())))
        }
    }

    pub(crate) fn expect_identifier(&mut self) -> Result<String> {
        match self.current_token().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }
    }

    pub(crate) fn current_token(&self) -> &Token {
        &self.tokens[self.position]
    }

    pub(crate) fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }
    fn parse_intersect(&mut self, left: Statement) -> Result<Statement> {
        use crate::parser::ast::IntersectStmt;

        let left_select = match left {
            Statement::Select(s) => s,
            _ => return Err(ParseError::InvalidSyntax("INTERSECT requires SELECT".to_string())),
        };

        self.expect(Token::Intersect)?;

        let right = select::parse_select(self)?;
        let right_select = match right {
            Statement::Select(s) => s,
            _ => return Err(ParseError::InvalidSyntax("INTERSECT requires SELECT".to_string())),
        };

        Ok(Statement::Intersect(IntersectStmt {
            left: Box::new(left_select),
            right: Box::new(right_select),
        }))
    }

    fn parse_except(&mut self, left: Statement) -> Result<Statement> {
        use crate::parser::ast::ExceptStmt;

        let left_select = match left {
            Statement::Select(s) => s,
            _ => return Err(ParseError::InvalidSyntax("EXCEPT requires SELECT".to_string())),
        };

        self.expect(Token::Except)?;

        let right = select::parse_select(self)?;
        let right_select = match right {
            Statement::Select(s) => s,
            _ => return Err(ParseError::InvalidSyntax("EXCEPT requires SELECT".to_string())),
        };

        Ok(Statement::Except(ExceptStmt {
            left: Box::new(left_select),
            right: Box::new(right_select),
        }))
    }

    fn parse_with(&mut self) -> Result<Statement> {
        use crate::parser::ast::{CTE, WithStmt};

        self.expect(Token::With)?;

        let recursive = if self.current_token() == &Token::Recursive {
            self.advance();
            true
        } else {
            false
        };

        let mut ctes = Vec::new();
        loop {
            let name = self.expect_identifier()?;
            self.expect(Token::As)?;
            self.expect(Token::LeftParen)?;
            let query = select::parse_select_stmt(self)?;
            self.expect(Token::RightParen)?;

            ctes.push(CTE { name, query: Box::new(query) });

            if self.current_token() != &Token::Comma {
                break;
            }
            self.advance();
        }

        let query = select::parse_select_stmt(self)?;

        Ok(Statement::With(WithStmt { recursive, ctes, query: Box::new(query) }))
    }

    fn parse_refresh(&mut self) -> Result<Statement> {
        use crate::parser::ast::RefreshMaterializedViewStmt;

        self.expect(Token::Refresh)?;
        self.expect(Token::Materialized)?;
        self.expect(Token::View)?;

        let name = self.expect_identifier()?;

        Ok(Statement::RefreshMaterializedView(RefreshMaterializedViewStmt { name }))
    }

    fn parse_prepare(&mut self) -> Result<Statement> {
        use crate::parser::ast::PrepareStmt;

        self.expect(Token::Prepare)?;
        let name = self.expect_identifier()?;
        self.expect(Token::As)?;
        let statement = Box::new(self.parse()?);

        Ok(Statement::Prepare(PrepareStmt { name, statement }))
    }

    fn parse_execute(&mut self) -> Result<Statement> {
        use crate::parser::ast::ExecuteStmt;

        self.expect(Token::Execute)?;
        let name = self.expect_identifier()?;

        let params = if self.current_token() == &Token::LeftParen {
            self.advance();
            let mut params = vec![];
            if self.current_token() != &Token::RightParen {
                params = expr::parse_expr_list(self)?;
            }
            self.expect(Token::RightParen)?;
            params
        } else {
            vec![]
        };

        Ok(Statement::Execute(ExecuteStmt { name, params }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let mut parser = Parser::new("SELECT * FROM users").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.from, "users");
                assert_eq!(s.columns, vec![Expr::Star]);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let mut parser = Parser::new("SELECT * FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.from, "users");
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Insert(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.values.len(), 2);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut parser = Parser::new("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Update(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.assignments.len(), 1);
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Delete(s) => {
                assert_eq!(s.table, "users");
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_with_semicolon() {
        let mut parser = Parser::new("SELECT * FROM users;").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.from, "users");
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_multiple_statements_with_semicolons() {
        let mut parser = Parser::new("SELECT 1;").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new("CREATE TABLE users (id INT, name TEXT)").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTable(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.columns.len(), 2);
                assert_eq!(s.columns[0].name, "id");
                assert_eq!(s.columns[1].name, "name");
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_describe() {
        let mut parser = Parser::new("DESCRIBE users").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Describe(s) => {
                assert_eq!(s.table, "users");
            }
            _ => panic!("Expected DESCRIBE statement"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let mut parser = Parser::new("DROP TABLE users").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::DropTable(s) => {
                assert_eq!(s.table, "users");
                assert!(!s.if_exists);
            }
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let mut parser = Parser::new("DROP TABLE IF EXISTS products").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::DropTable(s) => {
                assert_eq!(s.table, "products");
                assert!(s.if_exists);
            }
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn test_parse_where_with_comparison_operators() {
        let test_cases = vec![
            ("SELECT * FROM t WHERE x < 10", BinaryOperator::LessThan),
            ("SELECT * FROM t WHERE x <= 10", BinaryOperator::LessThanOrEqual),
            ("SELECT * FROM t WHERE x > 10", BinaryOperator::GreaterThan),
            ("SELECT * FROM t WHERE x >= 10", BinaryOperator::GreaterThanOrEqual),
            ("SELECT * FROM t WHERE x != 10", BinaryOperator::NotEquals),
        ];

        for (sql, expected_op) in test_cases {
            let mut parser = Parser::new(sql).unwrap();
            let stmt = parser.parse().unwrap();

            match stmt {
                Statement::Select(s) => {
                    assert!(s.where_clause.is_some());
                    match s.where_clause.unwrap() {
                        Expr::BinaryOp { op, .. } => assert_eq!(op, expected_op),
                        _ => panic!("Expected binary op"),
                    }
                }
                _ => panic!("Expected SELECT statement"),
            }
        }
    }
}
