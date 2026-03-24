use super::error::{ParseError, Result};

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
    Type,
    Int,
    Text,
    Varchar,
    Boolean,
    Date,
    Time,
    Timestamp,
    Decimal,
    Numeric,
    Bytea,
    Blob,
    Json,
    Jsonb,
    Enum,
    Array,
    Int4Range,
    Int8Range,
    NumRange,
    DateRange,
    TsRange,
    TsTzRange,
    Describe,
    Drop,
    Alter,
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
    Distinct,
    Like,
    In,
    Between,
    Not,
    Is,
    Null,
    Join,
    Lateral,
    Inner,
    Left,
    Right,
    Full,
    On,
    Union,
    All,
    Intersect,
    Except,
    With,
    Recursive,
    As,
    Over,
    Partition,
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
    Case,
    When,
    Then,
    Else,
    End,
    View,
    Materialized,
    Refresh,
    Trigger,
    Before,
    After,
    For,
    Each,
    Row,
    Statement,
    Begin,
    Commit,
    Rollback,
    Savepoint,
    Release,
    To,
    Transaction,
    Isolation,
    Level,
    Read,
    Committed,
    Repeatable,
    Serializable,
    Prepare,
    Execute,
    Deallocate,
    Index,
    Unique,
    Function,
    Procedure,
    Returns,
    Language,
    Sql,
    PlPgSql,
    Declare,
    Cursor,
    Fetch,
    Close,
    Next,
    Prior,
    First,
    Last,
    Absolute,
    Relative,
    Forward,
    Backward,
    Setof,
    Variadic,
    Inout,
    Out,
    Immutable,
    Stable,
    Volatile,
    Cost,
    Rows,
    Primary,
    Key,
    Foreign,
    References,
    Default,
    Serial,
    AutoIncrement,
    Cascade,
    Restrict,
    Add,

    // Identifiers and literals
    Identifier(String),
    Number(i64),
    String(String),
    Parameter(usize),

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
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
    Dot,
    Arrow,
    DoubleArrow,
    HashArrow,
    HashDoubleArrow,
    Question,
    QuestionBar,
    QuestionAmpersand,
    LeftBracket,
    RightBracket,
    AtSign,
    AndAnd,
    PipePipe,
    AtGreater,
    LessAt,

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
        Self { input: input.chars().collect(), position: 0 }
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
            '+' => {
                self.advance();
                Ok(Token::Plus)
            }
            '-' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '>' {
                    self.advance();
                    if !self.is_eof() && self.current_char() == '>' {
                        self.advance();
                        Ok(Token::DoubleArrow)
                    } else {
                        Ok(Token::Arrow)
                    }
                } else {
                    Ok(Token::Minus)
                }
            }
            '*' => {
                self.advance();
                Ok(Token::Star)
            }
            '/' => {
                self.advance();
                Ok(Token::Slash)
            }
            '%' => {
                self.advance();
                Ok(Token::Percent)
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
                } else if !self.is_eof() && self.current_char() == '@' {
                    self.advance();
                    Ok(Token::LessAt)
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
            '#' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '>' {
                    self.advance();
                    if !self.is_eof() && self.current_char() == '>' {
                        self.advance();
                        Ok(Token::HashDoubleArrow)
                    } else {
                        Ok(Token::HashArrow)
                    }
                } else {
                    Err(ParseError::UnexpectedToken("#".to_string()))
                }
            }
            '?' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '|' {
                    self.advance();
                    Ok(Token::QuestionBar)
                } else if !self.is_eof() && self.current_char() == '&' {
                    self.advance();
                    Ok(Token::QuestionAmpersand)
                } else {
                    Ok(Token::Question)
                }
            }
            '[' => {
                self.advance();
                Ok(Token::LeftBracket)
            }
            ']' => {
                self.advance();
                Ok(Token::RightBracket)
            }
            '@' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '>' {
                    self.advance();
                    Ok(Token::AtGreater)
                } else {
                    Err(ParseError::UnexpectedToken("@".to_string()))
                }
            }
            '&' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '&' {
                    self.advance();
                    Ok(Token::AndAnd)
                } else {
                    Err(ParseError::UnexpectedToken("&".to_string()))
                }
            }
            '|' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '|' {
                    self.advance();
                    Ok(Token::PipePipe)
                } else {
                    Err(ParseError::UnexpectedToken("|".to_string()))
                }
            }
            '.' => {
                self.advance();
                Ok(Token::Dot)
            }
            '\'' => self.read_string(),
            '$' => self.read_parameter(),
            _ if ch.is_ascii_digit() => self.read_number(),
            _ if ch.is_ascii_alphabetic() || ch == '_' => self.read_identifier(),
            _ => Err(ParseError::UnexpectedToken(ch.to_string())),
        }
    }

    fn read_identifier(&mut self) -> Result<Token> {
        let mut ident = String::new();

        while !self.is_eof()
            && (self.current_char().is_alphanumeric() || self.current_char() == '_')
        {
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
            "TYPE" => Token::Type,
            "INT" => Token::Int,
            "TEXT" => Token::Text,
            "VARCHAR" => Token::Varchar,
            "BOOLEAN" => Token::Boolean,
            "BOOL" => Token::Boolean,
            "DATE" => Token::Date,
            "TIME" => Token::Time,
            "TIMESTAMP" => Token::Timestamp,
            "DECIMAL" => Token::Decimal,
            "NUMERIC" => Token::Numeric,
            "BYTEA" => Token::Bytea,
            "BLOB" => Token::Blob,
            "JSON" => Token::Json,
            "JSONB" => Token::Jsonb,
            "ENUM" => Token::Enum,
            "ARRAY" => Token::Array,
            "INT4RANGE" => Token::Int4Range,
            "INT8RANGE" => Token::Int8Range,
            "NUMRANGE" => Token::NumRange,
            "DATERANGE" => Token::DateRange,
            "TSRANGE" => Token::TsRange,
            "TSTZRANGE" => Token::TsTzRange,
            "DESCRIBE" => Token::Describe,
            "DROP" => Token::Drop,
            "ALTER" => Token::Alter,
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
            "DISTINCT" => Token::Distinct,
            "LIKE" => Token::Like,
            "IN" => Token::In,
            "BETWEEN" => Token::Between,
            "NOT" => Token::Not,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "JOIN" => Token::Join,
            "LATERAL" => Token::Lateral,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "ON" => Token::On,
            "UNION" => Token::Union,
            "ALL" => Token::All,
            "INTERSECT" => Token::Intersect,
            "EXCEPT" => Token::Except,
            "WITH" => Token::With,
            "RECURSIVE" => Token::Recursive,
            "AS" => Token::As,
            "OVER" => Token::Over,
            "PARTITION" => Token::Partition,
            "ROW_NUMBER" => Token::RowNumber,
            "RANK" => Token::Rank,
            "DENSE_RANK" => Token::DenseRank,
            "LAG" => Token::Lag,
            "LEAD" => Token::Lead,
            "CASE" => Token::Case,
            "WHEN" => Token::When,
            "THEN" => Token::Then,
            "ELSE" => Token::Else,
            "END" => Token::End,
            "VIEW" => Token::View,
            "MATERIALIZED" => Token::Materialized,
            "REFRESH" => Token::Refresh,
            "TRIGGER" => Token::Trigger,
            "BEFORE" => Token::Before,
            "AFTER" => Token::After,
            "FOR" => Token::For,
            "EACH" => Token::Each,
            "ROW" => Token::Row,
            "STATEMENT" => Token::Statement,
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            "SAVEPOINT" => Token::Savepoint,
            "RELEASE" => Token::Release,
            "TO" => Token::To,
            "TRANSACTION" => Token::Transaction,
            "ISOLATION" => Token::Isolation,
            "LEVEL" => Token::Level,
            "READ" => Token::Read,
            "COMMITTED" => Token::Committed,
            "REPEATABLE" => Token::Repeatable,
            "SERIALIZABLE" => Token::Serializable,
            "PREPARE" => Token::Prepare,
            "EXECUTE" => Token::Execute,
            "DEALLOCATE" => Token::Deallocate,
            "INDEX" => Token::Index,
            "UNIQUE" => Token::Unique,
            "FUNCTION" => Token::Function,
            "PROCEDURE" => Token::Procedure,
            "RETURNS" => Token::Returns,
            "LANGUAGE" => Token::Language,
            "SQL" => Token::Sql,
            "PLPGSQL" => Token::PlPgSql,
            "DECLARE" => Token::Declare,
            "CURSOR" => Token::Cursor,
            "FETCH" => Token::Fetch,
            "CLOSE" => Token::Close,
            "NEXT" => Token::Next,
            "PRIOR" => Token::Prior,
            "FIRST" => Token::First,
            "LAST" => Token::Last,
            "ABSOLUTE" => Token::Absolute,
            "RELATIVE" => Token::Relative,
            "FORWARD" => Token::Forward,
            "BACKWARD" => Token::Backward,
            "SETOF" => Token::Setof,
            "VARIADIC" => Token::Variadic,
            "INOUT" => Token::Inout,
            "OUT" => Token::Out,
            "IMMUTABLE" => Token::Immutable,
            "STABLE" => Token::Stable,
            "VOLATILE" => Token::Volatile,
            "COST" => Token::Cost,
            "ROWS" => Token::Rows,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "FOREIGN" => Token::Foreign,
            "REFERENCES" => Token::References,
            "DEFAULT" => Token::Default,
            "SERIAL" => Token::Serial,
            "AUTO_INCREMENT" => Token::AutoIncrement,
            "ADD" => Token::Add,
            "CASCADE" => Token::Cascade,
            "RESTRICT" => Token::Restrict,
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

        let value = num
            .parse::<i64>()
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

    fn read_parameter(&mut self) -> Result<Token> {
        self.advance(); // Skip $
        let mut num = String::new();

        while !self.is_eof() && self.current_char().is_ascii_digit() {
            num.push(self.current_char());
            self.advance();
        }

        if num.is_empty() {
            return Err(ParseError::InvalidSyntax("Expected number after $".to_string()));
        }

        let value = num
            .parse::<usize>()
            .map_err(|_| ParseError::InvalidSyntax(format!("invalid parameter: ${}", num)))?;

        Ok(Token::Parameter(value))
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

    // --- New tests ---

    #[test]
    fn test_tokenize_all_keywords() {
        let mut lexer = Lexer::new(
            "SELECT INSERT UPDATE DELETE FROM WHERE INTO VALUES SET CREATE TABLE INT TEXT VARCHAR BOOLEAN DATE TIME TIMESTAMP DECIMAL NUMERIC BYTEA BLOB DESCRIBE DROP IF EXISTS ORDER BY ASC DESC LIMIT OFFSET COUNT SUM AVG MIN MAX AND OR GROUP HAVING DISTINCT LIKE IN BETWEEN NOT IS NULL JOIN LATERAL INNER LEFT RIGHT FULL ON UNION ALL INTERSECT EXCEPT WITH RECURSIVE AS OVER PARTITION ROW_NUMBER RANK DENSE_RANK LAG LEAD CASE WHEN THEN ELSE END VIEW MATERIALIZED REFRESH TRIGGER BEFORE AFTER FOR EACH ROW STATEMENT BEGIN COMMIT ROLLBACK SAVEPOINT RELEASE TO TRANSACTION ISOLATION LEVEL READ COMMITTED REPEATABLE SERIALIZABLE PREPARE EXECUTE DEALLOCATE INDEX UNIQUE FUNCTION PROCEDURE RETURNS LANGUAGE SQL PLPGSQL DECLARE CURSOR FETCH CLOSE NEXT PRIOR FIRST LAST ABSOLUTE RELATIVE FORWARD BACKWARD SETOF VARIADIC INOUT OUT IMMUTABLE STABLE VOLATILE COST ROWS PRIMARY KEY FOREIGN REFERENCES DEFAULT SERIAL AUTO_INCREMENT",
        );
        let tokens = lexer.tokenize().unwrap();
        let expected_tokens = vec![
            Token::Select,
            Token::Insert,
            Token::Update,
            Token::Delete,
            Token::From,
            Token::Where,
            Token::Into,
            Token::Values,
            Token::Set,
            Token::Create,
            Token::Table,
            Token::Int,
            Token::Text,
            Token::Varchar,
            Token::Boolean,
            Token::Date,
            Token::Time,
            Token::Timestamp,
            Token::Decimal,
            Token::Numeric,
            Token::Bytea,
            Token::Blob,
            Token::Describe,
            Token::Drop,
            Token::If,
            Token::Exists,
            Token::Order,
            Token::By,
            Token::Asc,
            Token::Descending,
            Token::Limit,
            Token::Offset,
            Token::Count,
            Token::Sum,
            Token::Avg,
            Token::Min,
            Token::Max,
            Token::And,
            Token::Or,
            Token::Group,
            Token::Having,
            Token::Distinct,
            Token::Like,
            Token::In,
            Token::Between,
            Token::Not,
            Token::Is,
            Token::Null,
            Token::Join,
            Token::Lateral,
            Token::Inner,
            Token::Left,
            Token::Right,
            Token::Full,
            Token::On,
            Token::Union,
            Token::All,
            Token::Intersect,
            Token::Except,
            Token::With,
            Token::Recursive,
            Token::As,
            Token::Over,
            Token::Partition,
            Token::RowNumber,
            Token::Rank,
            Token::DenseRank,
            Token::Lag,
            Token::Lead,
            Token::Case,
            Token::When,
            Token::Then,
            Token::Else,
            Token::End,
            Token::View,
            Token::Materialized,
            Token::Refresh,
            Token::Trigger,
            Token::Before,
            Token::After,
            Token::For,
            Token::Each,
            Token::Row,
            Token::Statement,
            Token::Begin,
            Token::Commit,
            Token::Rollback,
            Token::Savepoint,
            Token::Release,
            Token::To,
            Token::Transaction,
            Token::Isolation,
            Token::Level,
            Token::Read,
            Token::Committed,
            Token::Repeatable,
            Token::Serializable,
            Token::Prepare,
            Token::Execute,
            Token::Deallocate,
            Token::Index,
            Token::Unique,
            Token::Function,
            Token::Procedure,
            Token::Returns,
            Token::Language,
            Token::Sql,
            Token::PlPgSql,
            Token::Declare,
            Token::Cursor,
            Token::Fetch,
            Token::Close,
            Token::Next,
            Token::Prior,
            Token::First,
            Token::Last,
            Token::Absolute,
            Token::Relative,
            Token::Forward,
            Token::Backward,
            Token::Setof,
            Token::Variadic,
            Token::Inout,
            Token::Out,
            Token::Immutable,
            Token::Stable,
            Token::Volatile,
            Token::Cost,
            Token::Rows,
            Token::Primary,
            Token::Key,
            Token::Foreign,
            Token::References,
            Token::Default,
            Token::Serial,
            Token::AutoIncrement,
            Token::EOF,
        ];
        assert_eq!(tokens, expected_tokens);
    }

    #[test]
    fn test_tokenize_empty_input() {
        let mut lexer = Lexer::new("");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::EOF]);
    }

    #[test]
    fn test_tokenize_whitespace_only_input() {
        let mut lexer = Lexer::new("   \t\n\r");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::EOF]);
    }

    #[test]
    fn test_tokenize_identifiers_with_numbers_and_underscores() {
        let mut lexer = Lexer::new("col_1 table_name2 _id");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Identifier("col_1".to_string()),
                Token::Identifier("table_name2".to_string()),
                Token::Identifier("_id".to_string()),
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_tokenize_keywords_case_insensitivity() {
        let mut lexer = Lexer::new("select From WHERE");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::Select, Token::From, Token::Where, Token::EOF,]);
    }

    #[test]
    fn test_tokenize_string_with_special_chars() {
        let mut lexer = Lexer::new("'Hello, World! 123 _ -'");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::String("Hello, World! 123 _ -".to_string()), Token::EOF,]);
    }

    #[test]
    fn test_tokenize_empty_string() {
        let mut lexer = Lexer::new("''");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::String("".to_string()), Token::EOF,]);
    }

    #[test]
    fn test_tokenize_unterminated_string_error() {
        let mut lexer = Lexer::new("'unterminated");
        let result = lexer.tokenize();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ParseError::UnexpectedEOF);
    }

    #[test]
    fn test_tokenize_parameters() {
        let mut lexer = Lexer::new("$1 $10 $123");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![Token::Parameter(1), Token::Parameter(10), Token::Parameter(123), Token::EOF,]
        );
    }

    #[test]
    fn test_tokenize_invalid_parameter_error() {
        let mut lexer = Lexer::new("$abc");
        let result = lexer.tokenize();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ParseError::InvalidSyntax("Expected number after $".to_string())
        );
    }

    #[test]
    fn test_tokenize_dot_operator() {
        let mut lexer = Lexer::new("mytable.mycolumn");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Identifier("mytable".to_string()),
                Token::Dot,
                Token::Identifier("mycolumn".to_string()),
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_tokenize_invalid_character_error() {
        let mut lexer = Lexer::new("#");
        let result = lexer.tokenize();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ParseError::UnexpectedToken("#".to_string()));
    }

    #[test]
    fn test_tokenize_operator_error() {
        let mut lexer = Lexer::new("!x");
        let result = lexer.tokenize();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ParseError::UnexpectedToken("!".to_string()));
    }

    #[test]
    fn test_tokenize_mixed_tokens_and_whitespace() {
        let mut lexer = Lexer::new("  SELECT\n\t*   FROM  'my_table'  ; ");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::String("my_table".to_string()),
                Token::Semicolon,
                Token::EOF,
            ]
        );
    }

    #[test]
    fn test_tokenize_numbers_followed_by_identifier() {
        let mut lexer = Lexer::new("123abc");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![Token::Number(123), Token::Identifier("abc".to_string()), Token::EOF,]
        );
    }
}
