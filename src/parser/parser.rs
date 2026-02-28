use super::error::{Result, ParseError};
use super::lexer::{Lexer, Token};
use super::ast::*;

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
        let stmt = match self.current_token() {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Describe | Token::Desc => self.parse_describe(),
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }?;
        
        // Skip optional semicolon
        if self.current_token() == &Token::Semicolon {
            self.advance();
        }
        
        Ok(stmt)
    }
    
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(Token::Select)?;
        
        let columns = self.parse_select_list()?;
        
        // Handle SELECT without FROM (e.g., SELECT 1)
        let from = if self.current_token() == &Token::From {
            self.advance();
            self.expect_identifier()?
        } else {
            String::new()
        };
        
        let where_clause = if self.current_token() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        let order_by = if self.current_token() == &Token::Order {
            self.advance();
            self.expect(Token::By)?;
            Some(self.parse_order_by_list()?)
        } else {
            None
        };
        
        Ok(Statement::Select(SelectStmt {
            columns,
            from,
            where_clause,
            order_by,
        }))
    }
    
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        
        let table = self.expect_identifier()?;
        
        self.expect(Token::Values)?;
        self.expect(Token::LeftParen)?;
        
        let values = self.parse_expr_list()?;
        
        self.expect(Token::RightParen)?;
        
        Ok(Statement::Insert(InsertStmt {
            table,
            values,
        }))
    }
    
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(Token::Update)?;
        
        let table = self.expect_identifier()?;
        
        self.expect(Token::Set)?;
        
        let assignments = self.parse_assignments()?;
        
        let where_clause = if self.current_token() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        Ok(Statement::Update(UpdateStmt {
            table,
            assignments,
            where_clause,
        }))
    }
    
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        
        let table = self.expect_identifier()?;
        
        let where_clause = if self.current_token() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };
        
        Ok(Statement::Delete(DeleteStmt {
            table,
            where_clause,
        }))
    }
    
    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;
        
        let table = self.expect_identifier()?;
        
        self.expect(Token::LeftParen)?;
        
        let columns = self.parse_column_defs()?;
        
        self.expect(Token::RightParen)?;
        
        Ok(Statement::CreateTable(CreateTableStmt {
            table,
            columns,
        }))
    }
    
    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut columns = vec![self.parse_column_def()?];
        
        while self.current_token() == &Token::Comma {
            self.advance();
            columns.push(self.parse_column_def()?);
        }
        
        Ok(columns)
    }
    
    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.expect_identifier()?;
        let data_type = self.parse_data_type()?;
        
        Ok(ColumnDef { name, data_type })
    }
    
    fn parse_data_type(&mut self) -> Result<DataType> {
        match self.current_token() {
            Token::Int => {
                self.advance();
                Ok(DataType::Int)
            }
            Token::Text => {
                self.advance();
                Ok(DataType::Text)
            }
            Token::Varchar => {
                self.advance();
                if self.current_token() == &Token::LeftParen {
                    self.advance();
                    if let Token::Number(n) = self.current_token() {
                        let size = *n as u32;
                        self.advance();
                        self.expect(Token::RightParen)?;
                        Ok(DataType::Varchar(size))
                    } else {
                        Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token())))
                    }
                } else {
                    Ok(DataType::Varchar(255))
                }
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }
    }
    
    fn parse_describe(&mut self) -> Result<Statement> {
        // Accept both DESCRIBE and DESC
        if self.current_token() == &Token::Describe || self.current_token() == &Token::Desc {
            self.advance();
        }
        
        let table = self.expect_identifier()?;
        
        Ok(Statement::Describe(DescribeStmt { table }))
    }
    
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(Token::Drop)?;
        self.expect(Token::Table)?;
        
        // Check for IF EXISTS
        let if_exists = if self.current_token() == &Token::If {
            self.advance();
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };
        
        let table = self.expect_identifier()?;
        
        Ok(Statement::DropTable(DropTableStmt { table, if_exists }))
    }
    
    fn parse_select_list(&mut self) -> Result<Vec<Expr>> {
        if self.current_token() == &Token::Star {
            self.advance();
            return Ok(vec![Expr::Star]);
        }
        
        self.parse_expr_list()
    }
    
    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![self.parse_expr()?];
        
        while self.current_token() == &Token::Comma {
            self.advance();
            exprs.push(self.parse_expr()?);
        }
        
        Ok(exprs)
    }
    
    fn parse_assignments(&mut self) -> Result<Vec<(String, Expr)>> {
        let mut assignments = Vec::new();
        
        loop {
            let column = self.expect_identifier()?;
            self.expect(Token::Equals)?;
            let value = self.parse_expr()?;
            
            assignments.push((column, value));
            
            if self.current_token() != &Token::Comma {
                break;
            }
            self.advance();
        }
        
        Ok(assignments)
    }
    
    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>> {
        let mut order_by = Vec::new();
        
        loop {
            let column = self.expect_identifier()?;
            let ascending = match self.current_token() {
                Token::Descending => {
                    self.advance();
                    false
                }
                Token::Asc => {
                    self.advance();
                    true
                }
                _ => true, // Default to ASC
            };
            
            order_by.push(OrderByExpr { column, ascending });
            
            if self.current_token() != &Token::Comma {
                break;
            }
            self.advance();
        }
        
        Ok(order_by)
    }
    
    fn parse_expr(&mut self) -> Result<Expr> {
        let left = self.parse_primary()?;
        
        let op = match self.current_token() {
            Token::Equals => BinaryOperator::Equals,
            Token::NotEquals => BinaryOperator::NotEquals,
            Token::LessThan => BinaryOperator::LessThan,
            Token::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
            Token::GreaterThan => BinaryOperator::GreaterThan,
            Token::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
            _ => return Ok(left),
        };
        
        self.advance();
        let right = self.parse_primary()?;
        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }
    
    fn parse_primary(&mut self) -> Result<Expr> {
        match self.current_token().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(Expr::Column(name))
            }
            Token::Number(n) => {
                self.advance();
                Ok(Expr::Number(n))
            }
            Token::String(s) => {
                self.advance();
                Ok(Expr::String(s))
            }
            Token::Star => {
                self.advance();
                Ok(Expr::Star)
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }
    }
    
    fn expect(&mut self, expected: Token) -> Result<()> {
        if self.current_token() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token())))
        }
    }
    
    fn expect_identifier(&mut self) -> Result<String> {
        match self.current_token().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token()))),
        }
    }
    
    fn current_token(&self) -> &Token {
        &self.tokens[self.position]
    }
    
    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
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
