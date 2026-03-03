use crate::catalog::{Tuple, Value};
use crate::parser::ast::UniqueConstraint;

pub struct UniqueValidator;

impl UniqueValidator {
    pub fn validate(
        constraint: &UniqueConstraint,
        new_tuple: &[Value],
        existing_tuples: &[Tuple],
        column_indices: &[usize],
    ) -> Result<(), String> {
        let new_values: Vec<Value> =
            column_indices.iter().map(|&idx| new_tuple[idx].clone()).collect();

        for existing in existing_tuples {
            let existing_values: Vec<Value> =
                column_indices.iter().map(|&idx| existing.data[idx].clone()).collect();

            if new_values == existing_values {
                let name = constraint.name.as_deref().unwrap_or("unnamed");
                return Err(format!("UNIQUE constraint '{}' violated", name));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Tuple, Value};
    use crate::parser::ast::UniqueConstraint;
    use crate::transaction::TupleHeader;
    use std::collections::HashMap;

    fn create_tuple(values: Vec<Value>) -> Tuple {
        Tuple { header: TupleHeader::new(1), data: values, column_map: HashMap::new() }
    }

    #[test]
    fn test_unique_single_column() {
        let constraint = UniqueConstraint {
            name: Some("unique_email".to_string()),
            columns: vec!["email".to_string()],
        };

        let existing =
            vec![create_tuple(vec![Value::Int(1), Value::Text("alice@example.com".to_string())])];

        let new_tuple = vec![Value::Int(2), Value::Text("bob@example.com".to_string())];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[1]).is_ok());

        let duplicate = vec![Value::Int(2), Value::Text("alice@example.com".to_string())];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[1]).is_err());
    }

    #[test]
    fn test_unique_multiple_columns() {
        let constraint = UniqueConstraint {
            name: Some("unique_user_dept".to_string()),
            columns: vec!["user_id".to_string(), "dept_id".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Int(1), Value::Int(10)])];

        let new_tuple = vec![Value::Int(1), Value::Int(20)];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[0, 1]).is_ok());

        let duplicate = vec![Value::Int(1), Value::Int(10)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0, 1]).is_err());
    }

    #[test]
    fn test_unique_empty_table() {
        let constraint = UniqueConstraint {
            name: Some("unique_id".to_string()),
            columns: vec!["id".to_string()],
        };

        let new_tuple = vec![Value::Int(1)];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &[], &[0]).is_ok());
    }

    #[test]
    fn test_unique_text_values() {
        let constraint = UniqueConstraint {
            name: Some("unique_username".to_string()),
            columns: vec!["username".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Text("alice".to_string())])];

        let new_tuple = vec![Value::Text("bob".to_string())];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[0]).is_ok());

        let duplicate = vec![Value::Text("alice".to_string())];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_unnamed_constraint() {
        let constraint = UniqueConstraint { name: None, columns: vec!["id".to_string()] };

        let existing = vec![create_tuple(vec![Value::Int(1)])];
        let duplicate = vec![Value::Int(1)];

        let result = UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unnamed"));
    }

    #[test]
    fn test_unique_multiple_existing() {
        let constraint = UniqueConstraint {
            name: Some("unique_id".to_string()),
            columns: vec!["id".to_string()],
        };

        let existing = vec![
            create_tuple(vec![Value::Int(1)]),
            create_tuple(vec![Value::Int(2)]),
            create_tuple(vec![Value::Int(3)]),
        ];

        let new_tuple = vec![Value::Int(4)];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[0]).is_ok());

        let duplicate = vec![Value::Int(2)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;
    use crate::catalog::{Tuple, Value};
    use crate::parser::ast::UniqueConstraint;
    use crate::transaction::TupleHeader;
    use std::collections::HashMap;

    fn create_tuple(values: Vec<Value>) -> Tuple {
        Tuple { header: TupleHeader::new(1), data: values, column_map: HashMap::new() }
    }

    #[test]
    fn test_unique_zero_value() {
        let constraint = UniqueConstraint {
            name: Some("unique_val".to_string()),
            columns: vec!["val".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Int(0)])];
        let duplicate = vec![Value::Int(0)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_negative_value() {
        let constraint = UniqueConstraint {
            name: Some("unique_val".to_string()),
            columns: vec!["val".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Int(-1)])];
        let duplicate = vec![Value::Int(-1)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_empty_string() {
        let constraint = UniqueConstraint {
            name: Some("unique_text".to_string()),
            columns: vec!["text".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Text("".to_string())])];
        let duplicate = vec![Value::Text("".to_string())];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_large_number() {
        let constraint = UniqueConstraint {
            name: Some("unique_val".to_string()),
            columns: vec!["val".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Int(i64::MAX)])];
        let duplicate = vec![Value::Int(i64::MAX)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_unicode() {
        let constraint = UniqueConstraint {
            name: Some("unique_name".to_string()),
            columns: vec!["name".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Text("张三".to_string())])];
        let duplicate = vec![Value::Text("张三".to_string())];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_special_characters() {
        let constraint = UniqueConstraint {
            name: Some("unique_text".to_string()),
            columns: vec!["text".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Text("a'b\"c".to_string())])];
        let duplicate = vec![Value::Text("a'b\"c".to_string())];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_long_string() {
        let constraint = UniqueConstraint {
            name: Some("unique_text".to_string()),
            columns: vec!["text".to_string()],
        };

        let long_str = "a".repeat(10000);
        let existing = vec![create_tuple(vec![Value::Text(long_str.clone())])];
        let duplicate = vec![Value::Text(long_str)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_three_columns() {
        let constraint = UniqueConstraint {
            name: Some("unique_combo".to_string()),
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Int(1), Value::Int(2), Value::Int(3)])];

        let new_tuple = vec![Value::Int(1), Value::Int(2), Value::Int(4)];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[0, 1, 2]).is_ok());

        let duplicate = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0, 1, 2]).is_err());
    }

    #[test]
    fn test_unique_many_existing() {
        let constraint = UniqueConstraint {
            name: Some("unique_id".to_string()),
            columns: vec!["id".to_string()],
        };

        let existing: Vec<Tuple> = (0..1000).map(|i| create_tuple(vec![Value::Int(i)])).collect();

        let new_tuple = vec![Value::Int(1000)];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[0]).is_ok());

        let duplicate = vec![Value::Int(500)];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0]).is_err());
    }

    #[test]
    fn test_unique_mixed_types() {
        let constraint = UniqueConstraint {
            name: Some("unique_combo".to_string()),
            columns: vec!["id".to_string(), "name".to_string()],
        };

        let existing = vec![create_tuple(vec![Value::Int(1), Value::Text("alice".to_string())])];

        let new_tuple = vec![Value::Int(1), Value::Text("bob".to_string())];
        assert!(UniqueValidator::validate(&constraint, &new_tuple, &existing, &[0, 1]).is_ok());

        let duplicate = vec![Value::Int(1), Value::Text("alice".to_string())];
        assert!(UniqueValidator::validate(&constraint, &duplicate, &existing, &[0, 1]).is_err());
    }
}
