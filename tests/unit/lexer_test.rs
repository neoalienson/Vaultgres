use rustgres::parser::{Lexer, Token};

#[test]
fn test_lexer_keywords() {
    let mut lexer = Lexer::new("SELECT INSERT UPDATE DELETE");
    let tokens = lexer.tokenize().unwrap();
    
    assert_eq!(tokens[0], Token::Select);
    assert_eq!(tokens[1], Token::Insert);
    assert_eq!(tokens[2], Token::Update);
    assert_eq!(tokens[3], Token::Delete);
}

#[test]
fn test_lexer_identifiers() {
    let mut lexer = Lexer::new("users id name");
    let tokens = lexer.tokenize().unwrap();
    
    assert_eq!(tokens[0], Token::Identifier("users".to_string()));
    assert_eq!(tokens[1], Token::Identifier("id".to_string()));
    assert_eq!(tokens[2], Token::Identifier("name".to_string()));
}

#[test]
fn test_lexer_numbers() {
    let mut lexer = Lexer::new("123 456 789");
    let tokens = lexer.tokenize().unwrap();
    
    assert_eq!(tokens[0], Token::Number(123));
    assert_eq!(tokens[1], Token::Number(456));
    assert_eq!(tokens[2], Token::Number(789));
}

#[test]
fn test_lexer_strings() {
    let mut lexer = Lexer::new("'hello' 'world'");
    let tokens = lexer.tokenize().unwrap();
    
    assert_eq!(tokens[0], Token::String("hello".to_string()));
    assert_eq!(tokens[1], Token::String("world".to_string()));
}

#[test]
fn test_lexer_operators() {
    let mut lexer = Lexer::new("* , ; ( ) =");
    let tokens = lexer.tokenize().unwrap();
    
    assert_eq!(tokens[0], Token::Star);
    assert_eq!(tokens[1], Token::Comma);
    assert_eq!(tokens[2], Token::Semicolon);
    assert_eq!(tokens[3], Token::LeftParen);
    assert_eq!(tokens[4], Token::RightParen);
    assert_eq!(tokens[5], Token::Equals);
}

#[test]
fn test_lexer_case_insensitive() {
    let mut lexer1 = Lexer::new("select");
    let mut lexer2 = Lexer::new("SELECT");
    
    let tokens1 = lexer1.tokenize().unwrap();
    let tokens2 = lexer2.tokenize().unwrap();
    
    assert_eq!(tokens1[0], tokens2[0]);
}

#[test]
fn test_lexer_whitespace_handling() {
    let mut lexer = Lexer::new("  SELECT   *   FROM   users  ");
    let tokens = lexer.tokenize().unwrap();
    
    assert_eq!(tokens.len(), 5); // SELECT, *, FROM, users, EOF
}
