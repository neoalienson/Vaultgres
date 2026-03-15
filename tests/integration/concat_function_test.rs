// Integration tests for CONCAT, SUBSTRING, and other string functions
use vaultgres::catalog::Value;
use vaultgres::executor::Eval;

// ============= CONCAT Tests =============

#[test]
fn test_concat_basic_two_strings() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("hello".to_string()),
        Value::Text("world".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("helloworld".to_string()));
}

#[test]
fn test_concat_with_spaces() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("hello".to_string()),
        Value::Text(" ".to_string()),
        Value::Text("world".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("hello world".to_string()));
}

#[test]
fn test_concat_with_integer() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("Value: ".to_string()),
        Value::Int(42),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("Value: 42".to_string()));
}

#[test]
fn test_concat_mixed_types_sku_format() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("SKU".to_string()),
        Value::Text(" - ".to_string()),
        Value::Int(123),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("SKU - 123".to_string()));
}

#[test]
fn test_concat_with_null_skips_null() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("hello".to_string()),
        Value::Null,
        Value::Text("world".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("helloworld".to_string()));
}

#[test]
fn test_concat_all_nulls_returns_empty() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Null,
        Value::Null,
    ]).unwrap();
    
    assert_eq!(result, Value::Text("".to_string()));
}

#[test]
fn test_concat_empty_args_returns_empty() {
    let result = Eval::eval_function("CONCAT", vec![]).unwrap();
    
    assert_eq!(result, Value::Text("".to_string()));
}

#[test]
fn test_concat_single_string() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("single".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("single".to_string()));
}

#[test]
fn test_concat_single_int() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Int(999),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("999".to_string()));
}

#[test]
fn test_concat_invalid_type_bool() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Bool(true),
    ]);
    
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("CONCAT requires text or numeric values"));
}

#[test]
fn test_concat_five_args() {
    let result = Eval::eval_function("CONCAT", vec![
        Value::Text("a".to_string()),
        Value::Text("b".to_string()),
        Value::Text("c".to_string()),
        Value::Text("d".to_string()),
        Value::Text("e".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("abcde".to_string()));
}

#[test]
fn test_concat_pet_store_scenario() {
    // Simulates: SELECT CONCAT(sku, ' - ', name) FROM items WHERE is_current = 1
    let sku = Value::Text("DF001".to_string());
    let separator = Value::Text(" - ".to_string());
    let name = Value::Text("Premium Dog Food".to_string());
    
    let result = Eval::eval_function("CONCAT", vec![sku, separator, name]).unwrap();
    
    assert_eq!(result, Value::Text("DF001 - Premium Dog Food".to_string()));
}

#[test]
fn test_concat_complex_pet_store() {
    // Test various pet store SKU formats
    let test_cases = vec![
        (vec!["DF", "001"], "DF001"),
        (vec!["DF", " - ", "001"], "DF - 001"),
        (vec!["Premium ", "Dog ", "Food"], "Premium Dog Food"),
    ];
    
    for (inputs, expected) in test_cases {
        let args: Vec<Value> = inputs.into_iter()
            .map(|s| Value::Text(s.to_string()))
            .collect();
        let result = Eval::eval_function("CONCAT", args).unwrap();
        assert_eq!(result, Value::Text(expected.to_string()));
    }
}

// ============= SUBSTRING Tests =============

#[test]
fn test_substring_basic() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello world".to_string()),
        Value::Int(1),
        Value::Int(5),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("hello".to_string()));
}

#[test]
fn test_substring_from_middle() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello world".to_string()),
        Value::Int(7),
        Value::Int(5),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("world".to_string()));
}

#[test]
fn test_substring_without_length() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello world".to_string()),
        Value::Int(7),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("world".to_string()));
}

#[test]
fn test_substring_start_zero_treated_as_one() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello".to_string()),
        Value::Int(0),
        Value::Int(2),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("he".to_string()));
}

#[test]
fn test_substring_length_out_of_bounds() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello".to_string()),
        Value::Int(1),
        Value::Int(100),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("hello".to_string()));
}

#[test]
fn test_substring_start_out_of_bounds() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello".to_string()),
        Value::Int(10),
        Value::Int(2),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("".to_string()));
}

#[test]
fn test_substring_empty_string() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("".to_string()),
        Value::Int(1),
        Value::Int(5),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("".to_string()));
}

#[test]
fn test_substring_invalid_args_count() {
    let result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("hello".to_string()),
    ]);
    
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("SUBSTRING takes 2 or 3 arguments"));
}

#[test]
fn test_substring_email_masking() {
    // Simulates: SELECT SUBSTRING(email, 1, 5) FROM customers
    let email = Value::Text("john.doe@example.com".to_string());
    
    let result = Eval::eval_function("SUBSTRING", vec![
        email,
        Value::Int(1),
        Value::Int(5),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("john.".to_string()));
}

// ============= UPPER Tests =============

#[test]
fn test_upper_basic() {
    let result = Eval::eval_function("UPPER", vec![
        Value::Text("hello".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("HELLO".to_string()));
}

#[test]
fn test_upper_mixed_case() {
    let result = Eval::eval_function("UPPER", vec![
        Value::Text("Hello World".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("HELLO WORLD".to_string()));
}

#[test]
fn test_upper_already_upper() {
    let result = Eval::eval_function("UPPER", vec![
        Value::Text("HELLO".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("HELLO".to_string()));
}

#[test]
fn test_upper_empty_string() {
    let result = Eval::eval_function("UPPER", vec![
        Value::Text("".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("".to_string()));
}

#[test]
fn test_upper_with_numbers() {
    let result = Eval::eval_function("UPPER", vec![
        Value::Text("abc123def".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("ABC123DEF".to_string()));
}

// ============= LOWER Tests =============

#[test]
fn test_lower_basic() {
    let result = Eval::eval_function("LOWER", vec![
        Value::Text("HELLO".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("hello".to_string()));
}

#[test]
fn test_lower_mixed_case() {
    let result = Eval::eval_function("LOWER", vec![
        Value::Text("Hello World".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("hello world".to_string()));
}

#[test]
fn test_lower_already_lower() {
    let result = Eval::eval_function("LOWER", vec![
        Value::Text("hello".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("hello".to_string()));
}

#[test]
fn test_lower_empty_string() {
    let result = Eval::eval_function("LOWER", vec![
        Value::Text("".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("".to_string()));
}

// ============= LENGTH Tests =============

#[test]
fn test_length_basic() {
    let result = Eval::eval_function("LENGTH", vec![
        Value::Text("hello".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Int(5));
}

#[test]
fn test_length_empty_string() {
    let result = Eval::eval_function("LENGTH", vec![
        Value::Text("".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Int(0));
}

#[test]
fn test_length_with_spaces() {
    let result = Eval::eval_function("LENGTH", vec![
        Value::Text("hello world".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Int(11));
}

#[test]
fn test_length_sku_validation() {
    // Test SKU length validation
    let result = Eval::eval_function("LENGTH", vec![
        Value::Text("DF001".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Int(5));
}

// ============= COALESCE Tests =============

#[test]
fn test_coalesce_returns_first_non_null() {
    let result = Eval::eval_function("COALESCE", vec![
        Value::Null,
        Value::Null,
        Value::Text("found".to_string()),
        Value::Text("ignored".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("found".to_string()));
}

#[test]
fn test_coalesce_first_value() {
    let result = Eval::eval_function("COALESCE", vec![
        Value::Text("first".to_string()),
        Value::Null,
        Value::Text("second".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("first".to_string()));
}

#[test]
fn test_coalesce_all_nulls() {
    let result = Eval::eval_function("COALESCE", vec![
        Value::Null,
        Value::Null,
    ]).unwrap();
    
    assert_eq!(result, Value::Null);
}

#[test]
fn test_coalesce_single_value() {
    let result = Eval::eval_function("COALESCE", vec![
        Value::Text("only".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("only".to_string()));
}

#[test]
fn test_coalesce_single_null() {
    let result = Eval::eval_function("COALESCE", vec![
        Value::Null,
    ]).unwrap();
    
    assert_eq!(result, Value::Null);
}

// ============= NULLIF Tests =============

#[test]
fn test_nullif_equal_returns_null() {
    let result = Eval::eval_function("NULLIF", vec![
        Value::Text("same".to_string()),
        Value::Text("same".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Null);
}

#[test]
fn test_nullif_different_returns_first() {
    let result = Eval::eval_function("NULLIF", vec![
        Value::Text("first".to_string()),
        Value::Text("second".to_string()),
    ]).unwrap();
    
    assert_eq!(result, Value::Text("first".to_string()));
}

#[test]
fn test_nullif_both_null() {
    let result = Eval::eval_function("NULLIF", vec![
        Value::Null,
        Value::Null,
    ]).unwrap();
    
    assert_eq!(result, Value::Null);
}

#[test]
fn test_nullif_integers_equal() {
    let result = Eval::eval_function("NULLIF", vec![
        Value::Int(42),
        Value::Int(42),
    ]).unwrap();
    
    assert_eq!(result, Value::Null);
}

#[test]
fn test_nullif_integers_different() {
    let result = Eval::eval_function("NULLIF", vec![
        Value::Int(42),
        Value::Int(43),
    ]).unwrap();
    
    assert_eq!(result, Value::Int(42));
}

// ============= Combined Function Tests =============

#[test]
fn test_concat_then_upper() {
    // Test chaining: UPPER(CONCAT('hello', ' ', 'world'))
    let concat_result = Eval::eval_function("CONCAT", vec![
        Value::Text("hello".to_string()),
        Value::Text(" ".to_string()),
        Value::Text("world".to_string()),
    ]).unwrap();
    
    let upper_result = Eval::eval_function("UPPER", vec![concat_result]).unwrap();
    
    assert_eq!(upper_result, Value::Text("HELLO WORLD".to_string()));
}

#[test]
fn test_concat_then_length() {
    // Test chaining: LENGTH(CONCAT('hello', 'world'))
    let concat_result = Eval::eval_function("CONCAT", vec![
        Value::Text("hello".to_string()),
        Value::Text("world".to_string()),
    ]).unwrap();
    
    let length_result = Eval::eval_function("LENGTH", vec![concat_result]).unwrap();
    
    assert_eq!(length_result, Value::Int(10));
}

#[test]
fn test_substring_then_lower() {
    // Test chaining: LOWER(SUBSTRING('HELLO WORLD', 1, 5))
    let substring_result = Eval::eval_function("SUBSTRING", vec![
        Value::Text("HELLO WORLD".to_string()),
        Value::Int(1),
        Value::Int(5),
    ]).unwrap();
    
    let lower_result = Eval::eval_function("LOWER", vec![substring_result]).unwrap();
    
    assert_eq!(lower_result, Value::Text("hello".to_string()));
}

#[test]
fn test_pet_store_full_workflow() {
    // Simulate a full pet store data transformation workflow
    
    // 1. Create product display name: CONCAT(sku, ' - ', name)
    let display_name = Eval::eval_function("CONCAT", vec![
        Value::Text("DF001".to_string()),
        Value::Text(" - ".to_string()),
        Value::Text("Premium Dog Food".to_string()),
    ]).unwrap();
    assert_eq!(display_name, Value::Text("DF001 - Premium Dog Food".to_string()));
    
    // 2. Get uppercase category
    let upper_category = Eval::eval_function("UPPER", vec![
        Value::Text("food".to_string()),
    ]).unwrap();
    assert_eq!(upper_category, Value::Text("FOOD".to_string()));
    
    // 3. Get masked email for display
    let masked_email = Eval::eval_function("SUBSTRING", vec![
        Value::Text("customer@example.com".to_string()),
        Value::Int(1),
        Value::Int(8),
    ]).unwrap();
    assert_eq!(masked_email, Value::Text("customer".to_string()));
    
    // 4. Validate SKU length
    let sku_length = Eval::eval_function("LENGTH", vec![
        Value::Text("DF001".to_string()),
    ]).unwrap();
    assert_eq!(sku_length, Value::Int(5));
    
    // 5. Handle null price with coalesce
    let price_display = Eval::eval_function("COALESCE", vec![
        Value::Null,
        Value::Text("Call for price".to_string()),
    ]).unwrap();
    assert_eq!(price_display, Value::Text("Call for price".to_string()));
}
