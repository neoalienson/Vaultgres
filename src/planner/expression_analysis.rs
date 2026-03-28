use crate::catalog::Catalog;
use crate::parser::ast::{Expr, WindowFunc};

pub fn contains_aggregate(expr: &Expr, catalog: Option<&Catalog>) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::FunctionCall { name, args } => {
            if let Some(cat) = catalog {
                if cat.get_aggregate(name).is_some() {
                    return true;
                }
            }
            args.iter().any(|a| contains_aggregate(a, catalog))
        }
        Expr::Alias { expr, .. } => contains_aggregate(expr, catalog),
        _ => false,
    }
}

pub fn contains_window(expr: &Expr) -> bool {
    match expr {
        Expr::Window { .. } => true,
        Expr::FunctionCall { args, .. } => args.iter().any(contains_window),
        Expr::Alias { expr, .. } => contains_window(expr),
        _ => false,
    }
}

pub fn extract_window_exprs(
    columns: &[Expr],
) -> Vec<(
    usize,
    WindowFunc,
    Box<Expr>,
    Vec<String>,
    Vec<crate::parser::ast::OrderByExpr>,
    Option<crate::parser::ast::WindowFrame>,
)> {
    let mut window_exprs = Vec::new();
    for (idx, col) in columns.iter().enumerate() {
        if let Expr::Window { func, arg, partition_by, order_by, window_frame } = col {
            window_exprs.push((
                idx,
                func.clone(),
                arg.clone(),
                partition_by.clone(),
                order_by.clone(),
                window_frame.clone(),
            ));
        }
    }
    window_exprs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{AggregateFunc, BinaryOperator, UnaryOperator};

    #[test]
    fn test_contains_aggregate_with_aggregate() {
        let expr = Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) };
        assert!(contains_aggregate(&expr, None));
    }

    #[test]
    fn test_contains_aggregate_without_aggregate() {
        let expr = Expr::Column("id".to_string());
        assert!(!contains_aggregate(&expr, None));
    }

    #[test]
    fn test_contains_aggregate_nested() {
        let expr = Expr::Alias {
            alias: "cnt".to_string(),
            expr: Box::new(Expr::Aggregate {
                func: AggregateFunc::Count,
                arg: Box::new(Expr::Star),
            }),
        };
        assert!(contains_aggregate(&expr, None));
    }

    #[test]
    fn test_contains_window_with_window() {
        let expr = Expr::Window {
            func: WindowFunc::RowNumber,
            arg: Box::new(Expr::Star),
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        assert!(contains_window(&expr));
    }

    #[test]
    fn test_contains_window_without_window() {
        let expr = Expr::Column("id".to_string());
        assert!(!contains_window(&expr));
    }

    #[test]
    fn test_extract_window_exprs_empty() {
        let columns = vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())];
        let result = extract_window_exprs(&columns);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_window_exprs_with_windows() {
        let columns = vec![
            Expr::Column("id".to_string()),
            Expr::Window {
                func: WindowFunc::RowNumber,
                arg: Box::new(Expr::Star),
                partition_by: vec![],
                order_by: vec![],
                window_frame: None,
            },
            Expr::Column("name".to_string()),
        ];
        let result = extract_window_exprs(&columns);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 1);
    }
}
