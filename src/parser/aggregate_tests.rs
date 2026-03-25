#[cfg(test)]
mod tests {
    use crate::parser::ast::*;
    use crate::parser::parse;

    fn parse_create_aggregate(sql: &str) -> CreateAggregateStmt {
        match parse(sql).unwrap() {
            Statement::CreateAggregate(s) => s,
            _ => panic!("Expected CREATE AGGREGATE statement"),
        }
    }

    fn parse_drop_aggregate(sql: &str) -> DropAggregateStmt {
        match parse(sql).unwrap() {
            Statement::DropAggregate(s) => s,
            _ => panic!("Expected DROP AGGREGATE statement"),
        }
    }

    #[test]
    fn test_create_aggregate_basic() {
        let sql = "CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8)";
        let stmt = parse_create_aggregate(sql);
        assert_eq!(stmt.name, "my_avg");
        assert_eq!(stmt.input_type, "INT");
        assert_eq!(stmt.sfunc, "int8_avg_accum");
        assert_eq!(stmt.stype, "INT8");
        assert!(stmt.finalfunc.is_none());
        assert!(stmt.initcond.is_none());
    }

    #[test]
    fn test_create_aggregate_with_finalfunc() {
        let sql =
            "CREATE AGGREGATE my_sum (INT) (SFUNC = int8_sum, STYPE = INT8, FINALFUNC = int8_avg)";
        let stmt = parse_create_aggregate(sql);
        assert_eq!(stmt.name, "my_sum");
        assert_eq!(stmt.sfunc, "int8_sum");
        assert_eq!(stmt.stype, "INT8");
        assert_eq!(stmt.finalfunc, Some("int8_avg".to_string()));
    }

    #[test]
    fn test_create_aggregate_with_initcond() {
        let sql =
            "CREATE AGGREGATE my_count (INT) (SFUNC = int8_avg_accum, STYPE = INT8, INITCOND = 0)";
        let stmt = parse_create_aggregate(sql);
        assert_eq!(stmt.sfunc, "int8_avg_accum");
        assert_eq!(stmt.stype, "INT8");
        assert_eq!(stmt.initcond, Some("0".to_string()));
    }

    #[test]
    fn test_create_aggregate_with_volatility() {
        let sql = "CREATE AGGREGATE my_agg (INT) (SFUNC = my_sfunc, STYPE = INT) IMMUTABLE";
        let stmt = parse_create_aggregate(sql);
        assert_eq!(stmt.sfunc, "my_sfunc");
        assert_eq!(stmt.volatility, Some(FunctionVolatility::Immutable));
    }

    #[test]
    fn test_create_aggregate_with_string_initcond() {
        let sql =
            "CREATE AGGREGATE my_concat (TEXT) (SFUNC = text_concat, STYPE = TEXT, INITCOND = '')";
        let stmt = parse_create_aggregate(sql);
        assert_eq!(stmt.sfunc, "text_concat");
        assert_eq!(stmt.stype, "TEXT");
        assert_eq!(stmt.initcond, Some("".to_string()));
    }

    #[test]
    fn test_drop_aggregate_basic() {
        let sql = "DROP AGGREGATE my_avg";
        let stmt = parse_drop_aggregate(sql);
        assert_eq!(stmt.name, "my_avg");
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_drop_aggregate_if_exists() {
        let sql = "DROP AGGREGATE IF EXISTS my_avg";
        let stmt = parse_drop_aggregate(sql);
        assert_eq!(stmt.name, "my_avg");
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_drop_aggregate_case_insensitive() {
        let sql = "drop aggregate my_avg";
        let stmt = parse_drop_aggregate(sql);
        assert_eq!(stmt.name, "my_avg");
    }

    #[test]
    fn test_create_aggregate_case_insensitive() {
        let sql = "create aggregate my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8)";
        let stmt = parse_create_aggregate(sql);
        assert_eq!(stmt.name, "my_avg");
    }
}
