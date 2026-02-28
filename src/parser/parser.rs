use super::error::{Result, ParseError};
use super::lexer::{Lexer, Token};
use super::ast::*;

mod select;
mod dml;
mod ddl;
mod expr;

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
        
        Ok(Self {
            tokens,
            position: 0,
        })
    }
    
    /// Parses a SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        if self.current_token() == &Token::With {
            return self.parse_with();
        }
        
        if self.current_token() == &Token::Select {
            let select = select::parse_select(self)?;
            // Check for set operations
            match self.current_token() {
                Token::Union => return self.parse_union(select),
                Token::Intersect => return self.parse_intersect(select),
                Token::Except => return self.parse_except(select),
                _ => return Ok(select),
            }
        }
        
        if self.current_token() == &Token::Refresh {
            return self.parse_refresh();
        }
        
        let stmt = match self.current_token() {
            Token::Insert => dml::parse_insert(self),
            Token::Update => dml::parse_update(self),
            Token::Delete => dml::parse_delete(self),
            Token::Create => ddl::parse_create(self),
            Token::Drop => ddl::parse_drop(self),
            Token::Describe | Token::Desc => ddl::parse_describe(self),
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }?;
        
        // Skip optional semicolon
        if self.current_token() == &Token::Semicolon {
            self.advance();
        }
        
        Ok(stmt)
    }
    
    fn parse_union(&mut self, left: Statement) -> Result<Statement> {
        use crate::parser::ast::{UnionStmt, SelectStmt};
        
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
        use crate::parser::ast::{IntersectStmt, SelectStmt};
        
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
        use crate::parser::ast::{ExceptStmt, SelectStmt};
        
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
        use crate::parser::ast::{WithStmt, CTE};
        
        self.expect(Token::With)?;
        
        let mut ctes = Vec::new();
        loop {
            let name = self.expect_identifier()?;
            self.expect(Token::As)?;
            self.expect(Token::LeftParen)?;
            let query = select::parse_select_stmt(self)?;
            self.expect(Token::RightParen)?;
            
            ctes.push(CTE {
                name,
                query: Box::new(query),
            });
            
            if self.current_token() != &Token::Comma {
                break;
            }
            self.advance();
        }
        
        let query = select::parse_select_stmt(self)?;
        
        Ok(Statement::With(WithStmt {
            ctes,
            query: Box::new(query),
        }))
    }
    
    fn parse_refresh(&mut self) -> Result<Statement> {
        use crate::parser::ast::RefreshMaterializedViewStmt;
        
        self.expect(Token::Refresh)?;
        self.expect(Token::Materialized)?;
        self.expect(Token::View)?;
        
        let name = self.expect_identifier()?;
        
        Ok(Statement::RefreshMaterializedView(RefreshMaterializedViewStmt { name }))
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
    fn test_parse_desc() {
        let mut parser = Parser::new("DESC products").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::Describe(s) => {
                assert_eq!(s.table, "products");
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
                assert_eq!(s.if_exists, false);
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
                assert_eq!(s.if_exists, true);
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

    
