#[cfg(test)]
mod tests {
    use crate::catalog::value::Value;
    use crate::parser::ast::DataType;

    #[test]
    fn test_boolean_value() {
        let v1 = Value::Bool(true);
        let v2 = Value::Bool(false);
        assert_ne!(v1, v2);
        assert!(v1 > v2);
    }

    #[test]
    fn test_date_value() {
        let v1 = Value::Date(18000);
        let v2 = Value::Date(18001);
        assert!(v1 < v2);
    }

    #[test]
    fn test_time_value() {
        let v1 = Value::Time(3600_000_000);
        let v2 = Value::Time(7200_000_000);
        assert!(v1 < v2);
    }

    #[test]
    fn test_timestamp_value() {
        let v1 = Value::Timestamp(1609459200_000_000);
        let v2 = Value::Timestamp(1609545600_000_000);
        assert!(v1 < v2);
    }

    #[test]
    fn test_decimal_value() {
        let v1 = Value::Decimal(12345, 2);
        let v2 = Value::Decimal(12346, 2);
        assert!(v1 < v2);
    }

    #[test]
    fn test_bytea_value() {
        let v1 = Value::Bytea(vec![1, 2, 3]);
        let v2 = Value::Bytea(vec![1, 2, 4]);
        assert!(v1 < v2);
    }

    #[test]
    fn test_datatype_boolean() {
        let dt = DataType::Boolean;
        assert_eq!(dt, DataType::Boolean);
    }

    #[test]
    fn test_datatype_date() {
        let dt = DataType::Date;
        assert_eq!(dt, DataType::Date);
    }

    #[test]
    fn test_datatype_time() {
        let dt = DataType::Time;
        assert_eq!(dt, DataType::Time);
    }

    #[test]
    fn test_datatype_timestamp() {
        let dt = DataType::Timestamp;
        assert_eq!(dt, DataType::Timestamp);
    }

    #[test]
    fn test_datatype_decimal() {
        let dt = DataType::Decimal(10, 2);
        assert_eq!(dt, DataType::Decimal(10, 2));
    }

    #[test]
    fn test_datatype_bytea() {
        let dt = DataType::Bytea;
        assert_eq!(dt, DataType::Bytea);
    }

    #[test]
    fn test_range_value() {
        use crate::catalog::value::{Range, RangeBound};
        let r1 = Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), true);
        assert_eq!(r1.lower_bound(), Some(&Value::Int(1)));
        assert_eq!(r1.upper_bound(), Some(&Value::Int(5)));
        assert!(r1.lower_inclusive());
        assert!(r1.upper_inclusive());
    }

    #[test]
    fn test_range_empty() {
        use crate::catalog::value::Range;
        let r = Range::empty();
        assert!(r.lower_bound().is_none());
        assert!(r.upper_bound().is_none());
        assert!(!r.lower_inclusive());
        assert!(!r.upper_inclusive());
    }

    #[test]
    fn test_range_not_empty() {
        use crate::catalog::value::Range;
        let r = Range::new(Some(Value::Int(5)), true, Some(Value::Int(1)), true);
        assert!(r.is_empty());
    }

    #[test]
    fn test_range_exclusive_bounds() {
        use crate::catalog::value::Range;
        let r = Range::new(Some(Value::Int(1)), false, Some(Value::Int(5)), false);
        assert!(!r.lower_inclusive());
        assert!(!r.upper_inclusive());
    }

    #[test]
    fn test_range_with_null_bounds() {
        use crate::catalog::value::Range;
        let r = Range::new(None, true, Some(Value::Int(5)), true);
        assert!(r.lower_bound().is_none());
        assert!(r.upper_bound().is_some());
    }

    #[test]
    fn test_range_display() {
        use crate::catalog::value::Range;
        let r = Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), true);
        assert_eq!(format!("{}", r), "[1,5]");
    }

    #[test]
    fn test_range_display_exclusive() {
        use crate::catalog::value::Range;
        let r = Range::new(Some(Value::Int(1)), false, Some(Value::Int(5)), false);
        assert_eq!(format!("{}", r), "(1,5)");
    }

    #[test]
    fn test_datatype_int4range() {
        let dt = DataType::Int4Range;
        assert_eq!(dt, DataType::Int4Range);
    }

    #[test]
    fn test_datatype_int8range() {
        let dt = DataType::Int8Range;
        assert_eq!(dt, DataType::Int8Range);
    }

    #[test]
    fn test_datatype_numrange() {
        let dt = DataType::NumRange;
        assert_eq!(dt, DataType::NumRange);
    }

    #[test]
    fn test_datatype_daterange() {
        let dt = DataType::DateRange;
        assert_eq!(dt, DataType::DateRange);
    }

    #[test]
    fn test_datatype_tszrange() {
        let dt = DataType::TsRange;
        assert_eq!(dt, DataType::TsRange);
    }

    #[test]
    fn test_datatype_tstzrange() {
        let dt = DataType::TsTzRange;
        assert_eq!(dt, DataType::TsTzRange);
    }
}
