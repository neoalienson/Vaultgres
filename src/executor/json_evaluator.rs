use crate::catalog::Value;
use crate::executor::operators::executor::ExecutorError;

pub struct JsonEvaluator;

impl JsonEvaluator {
    pub fn eval_json_extract(
        left: &Value,
        right: &Value,
        as_text: bool,
    ) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON extract requires JSON or text operand".to_string(),
                ));
            }
        };

        let key = match right {
            Value::Text(k) => k.as_str(),
            Value::Int(i) => &i.to_string(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON extract key must be text or integer".to_string(),
                ));
            }
        };

        match Self::extract_json_field(json_str, key) {
            Some(value) => {
                if as_text {
                    Ok(Value::Text(value.to_string()))
                } else {
                    Ok(Value::Json(value.to_string()))
                }
            }
            None => Ok(Value::Null),
        }
    }

    pub fn extract_json_field(json_str: &str, key: &str) -> Option<String> {
        let json_str = json_str.trim();
        if json_str.starts_with('[') {
            if let Ok(idx) = key.parse::<usize>() {
                return Self::extract_json_array_element(json_str, idx);
            }
            return None;
        }
        if json_str.starts_with('{') {
            return Self::extract_json_object_field(json_str, key);
        }
        None
    }

    pub fn extract_json_array_element(json_str: &str, idx: usize) -> Option<String> {
        let json_str = json_str.trim();
        if !json_str.starts_with('[') || !json_str.ends_with(']') {
            return None;
        }
        let content = &json_str[1..json_str.len() - 1];
        if content.trim().is_empty() {
            return None;
        }
        let elements = Self::split_json_array(content);
        if idx >= elements.len() {
            return None;
        }
        Some(elements[idx].trim().to_string())
    }

    pub fn split_json_array(content: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;
        let mut in_string = false;
        for (i, c) in content.chars().enumerate() {
            match c {
                '"' => in_string = !in_string,
                '[' | '{' if !in_string => depth += 1,
                ']' | '}' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    result.push(&content[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        result.push(&content[start..]);
        result
    }

    pub fn extract_json_object_field(json_str: &str, key: &str) -> Option<String> {
        let json_str = json_str.trim();
        if !json_str.starts_with('{') || !json_str.ends_with('}') {
            return None;
        }
        let content = &json_str[1..json_str.len() - 1];
        if content.trim().is_empty() {
            return None;
        }
        let pairs = Self::split_json_object(content);
        for pair in pairs {
            if let Some((k, v)) = Self::parse_json_pair(pair) {
                if k == key {
                    return Some(v);
                }
            }
        }
        None
    }

    pub fn split_json_object(content: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;
        let mut in_string = false;
        for (i, c) in content.chars().enumerate() {
            match c {
                '"' => in_string = !in_string,
                '{' | '[' if !in_string => depth += 1,
                '}' | ']' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    result.push(&content[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        result.push(&content[start..]);
        result
    }

    pub fn parse_json_pair(pair: &str) -> Option<(String, String)> {
        let pair = pair.trim();
        if !pair.starts_with('"') {
            return None;
        }
        let colon_pos = pair.find(':')?;
        let closing_quote_pos = pair[..colon_pos].rfind('"')?;
        let key = pair[1..closing_quote_pos].to_string();
        let raw_value = pair[colon_pos + 1..].trim();
        let value = if raw_value.starts_with('"') {
            let first_quote = raw_value.find('"')? + 1;
            let second_quote = raw_value[first_quote..].find('"')? + first_quote;
            raw_value[first_quote..second_quote].to_string()
        } else {
            raw_value.to_string()
        };
        Some((key, value))
    }

    pub fn eval_json_path(
        left: &Value,
        right: &Value,
        as_text: bool,
    ) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON path requires JSON or text operand".to_string(),
                ));
            }
        };

        let path = match right {
            Value::Text(p) => p.as_str(),
            _ => return Err(ExecutorError::TypeMismatch("JSON path must be text".to_string())),
        };

        let result = Self::extract_json_path(json_str, path);
        match result {
            Some(value) => {
                if as_text {
                    Ok(Value::Text(value.to_string()))
                } else {
                    Ok(Value::Json(value.to_string()))
                }
            }
            None => Ok(Value::Null),
        }
    }

    pub fn extract_json_path(json_str: &str, path: &str) -> Option<String> {
        let path = path.trim();
        if !path.starts_with('{') || !path.ends_with('}') {
            if !path.starts_with('[') || !path.ends_with(']') {
                return None;
            }
            let content = &path[1..path.len() - 1];
            let keys: Vec<&str> = content.split(',').map(|s| s.trim().trim_matches('"')).collect();
            let mut current = json_str.to_string();
            for key in keys {
                current = Self::extract_json_field(&current, key)?;
            }
            return Some(current);
        }
        let content = &path[1..path.len() - 1];
        let keys: Vec<&str> = content.split('.').map(|s| s.trim().trim_matches('"')).collect();
        let mut current = json_str.to_string();
        for key in keys {
            if key.starts_with('[') && key.ends_with(']') {
                let idx_str = &key[1..key.len() - 1];
                if let Ok(idx) = idx_str.parse::<usize>() {
                    current = Self::extract_json_array_element(&current, idx)?;
                } else {
                    return None;
                }
            } else {
                current = Self::extract_json_field(&current, key)?;
            }
        }
        Some(current)
    }

    pub fn eval_json_exists(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists requires JSON or text operand".to_string(),
                ));
            }
        };

        let key = match right {
            Value::Text(k) => k.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists key must be text".to_string(),
                ));
            }
        };

        Ok(Value::Bool(Self::extract_json_field(json_str, key).is_some()))
    }

    pub fn eval_json_exists_any(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists requires JSON or text operand".to_string(),
                ));
            }
        };

        let keys = match right {
            Value::Text(k) => k.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists keys must be text".to_string(),
                ));
            }
        };

        let keys = keys.trim();
        if !keys.starts_with('[') || !keys.ends_with(']') {
            return Err(ExecutorError::TypeMismatch(
                "JSON exists keys must be an array".to_string(),
            ));
        }

        let content = &keys[1..keys.len() - 1];
        if content.trim().is_empty() {
            return Ok(Value::Bool(false));
        }

        let key_list: Vec<&str> = content.split(',').map(|s| s.trim().trim_matches('"')).collect();
        for key in key_list {
            if Self::extract_json_field(json_str, key).is_some() {
                return Ok(Value::Bool(true));
            }
        }
        Ok(Value::Bool(false))
    }

    pub fn eval_json_exists_all(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let json_str = match left {
            Value::Json(j) => j.as_str(),
            Value::Text(j) => j.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists requires JSON or text operand".to_string(),
                ));
            }
        };

        let keys = match right {
            Value::Text(k) => k.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "JSON exists keys must be text".to_string(),
                ));
            }
        };

        let keys = keys.trim();
        if !keys.starts_with('[') || !keys.ends_with(']') {
            return Err(ExecutorError::TypeMismatch(
                "JSON exists keys must be an array".to_string(),
            ));
        }

        let content = &keys[1..keys.len() - 1];
        if content.trim().is_empty() {
            return Ok(Value::Bool(false));
        }

        let key_list: Vec<&str> = content.split(',').map(|s| s.trim().trim_matches('"')).collect();
        for key in key_list {
            if Self::extract_json_field(json_str, key).is_none() {
                return Ok(Value::Bool(false));
            }
        }
        Ok(Value::Bool(true))
    }
}
