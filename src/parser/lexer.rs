use super::error::{Result, ParseError};

/// Token type
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    From,
    Where,
    Into,
    Values,
    Set,
    Create,
    Table,
    Int,
    Text,
    Varchar,
    Describe,
    Desc,
    Drop,
    If,
    Exists,
    Order,
    By,
    Asc,
    Descending,
    Limit,
    Offset,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    And,
    Or,
    Group,
    Having,
    
    // Identifiers and literals
    Identifier(String),
    Number(i64),
    String(String),
    
    // Operators
    Star,
    Comma,
    Semicolon,
    LeftParen,
    RightParen,
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    
    // End of input
    EOF,
}

/// Lexer for SQL tokenization
pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    /// Creates a new lexer
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }
    
    /// Tokenizes the input
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        
        loop {
            self.skip_whitespace();
            
            if self.is_eof() {
                tokens.push(Token::EOF);
                break;
            }
            
            let token = self.next_token()?;
            tokens.push(token);
        }
        
        Ok(tokens)
    }
    
    fn next_token(&mut self) -> Result<Token> {
        let ch = self.current_char();
        
        match ch {
            '*' => {
                self.advance();
                Ok(Token::Star)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            ';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            '(' => {
                self.advance();
                Ok(Token::LeftParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RightParen)
            }
            '=' => {
                self.advance();
                Ok(Token::Equals)
            }
            '!' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    Ok(Token::NotEquals)
                } else {
                    Err(ParseError::UnexpectedToken("!".to_string()))
                }
            }
            '<' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '=' {
                    self.advance();
                    Ok(Token::LessThanOrEqual)
                } else {
                    Ok(Token::LessThan)
                }
            }
            '>' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '=' {
                    self.advance();
                    Ok(Token::GreaterThanOrEqual)
                } else {
                    Ok(Token::GreaterThan)
                }
            }
            '\'' => self.read_string(),
            _ if ch.is_ascii_digit() => self.read_number(),
            _ if ch.is_ascii_alphabetic() => self.read_identifier(),
            _ => Err(ParseError::UnexpectedToken(ch.to_string())),
        }
    }
    
    fn read_identifier(&mut self) -> Result<Token> {
        let mut ident = String::new();
        
        while !self.is_eof() && (self.current_char().is_alphanumeric() || self.current_char() == '_') {
            ident.push(self.current_char());
            self.advance();
        }
        
        let token = match ident.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "INSERT" => Token::Insert,
            "UPDATE" => Token::Update,
            "DELETE" => Token::Delete,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SET" => Token::Set,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "INT" => Token::Int,
            "TEXT" => Token::Text,
            "VARCHAR" => Token::Varchar,
            "DESCRIBE" => Token::Describe,
            "DESC" => Token::Desc,
            "DROP" => Token::Drop,
            "IF" => Token::If,
            "EXISTS" => Token::Exists,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Descending,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "COUNT" => Token::Count,
            "SUM" => Token::Sum,
            "AVG" => Token::Avg,
            "MIN" => Token::Min,
            "MAX" => Token::Max,
            "AND" => Token::And,
            "OR" => Token::Or,
            "GROUP" => Token::Group,
            "HAVING" => Token::Having,
            _ => Token::Identifier(ident),
        };
        
        Ok(token)
    }
    
    fn read_number(&mut self) -> Result<Token> {
        let mut num = String::new();
        
        while !self.is_eof() && self.current_char().is_ascii_digit() {
            num.push(self.current_char());
            self.advance();
        }
        
        let value = num.parse::<i64>()
            .map_err(|_| ParseError::InvalidSyntax(format!("invalid number: {}", num)))?;
        
        Ok(Token::Number(value))
    }
    
    fn read_string(&mut self) -> Result<Token> {
        self.advance(); // Skip opening quote
        let mut s = String::new();
        
        while !self.is_eof() && self.current_char() != '\'' {
            s.push(self.current_char());
            self.advance();
        }
        
        if self.is_eof() {
            return Err(ParseError::UnexpectedEOF);
        }
        
        self.advance(); // Skip closing quote
        Ok(Token::String(s))
    }
    
    fn skip_whitespace(&mut self) {
        while !self.is_eof() && self.current_char().is_whitespace() {
            self.advance();
        }
    }
    
    fn current_char(&self) -> char {
        self.input[self.position]
    }
    
    fn advance(&mut self) {
        self.position += 1;
    }
    
    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
    }
    
    #[test]
    fn test_tokenize_insert() {
        let mut lexer = Lexer::new("INSERT INTO users VALUES (1, 'Alice')");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0], Token::Insert);
        assert_eq!(tokens[1], Token::Into);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
    }
    
    #[test]
    fn test_tokenize_numbers() {
        let mut lexer = Lexer::new("123 456");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0], Token::Number(123));
        assert_eq!(tokens[1], Token::Number(456));
    }
    
    #[test]
    fn test_tokenize_strings() {
        let mut lexer = Lexer::new("'hello' 'world'");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0], Token::String("hello".to_string()));
        assert_eq!(tokens[1], Token::String("world".to_string()));
    }
    
    #[test]
    fn test_tokenize_comparison_operators() {
        let mut lexer = Lexer::new("= != < <= > >=");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0], Token::Equals);
        assert_eq!(tokens[1], Token::NotEquals);
        assert_eq!(tokens[2], Token::LessThan);
        assert_eq!(tokens[3], Token::LessThanOrEqual);
        assert_eq!(tokens[4], Token::GreaterThan);
        assert_eq!(tokens[5], Token::GreaterThanOrEqual);
    }
}
