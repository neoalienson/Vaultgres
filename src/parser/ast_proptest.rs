#[cfg(test)]
pub mod proptest_strategies {
    use crate::parser::ast::{
        AggregateFunc, BinaryOperator, DataType, Expr, OrderByExpr, UnaryOperator, WindowFunc,
    };
    use proptest::prelude::*;

    impl Arbitrary for BinaryOperator {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(BinaryOperator::Equals),
                Just(BinaryOperator::NotEquals),
                Just(BinaryOperator::LessThan),
                Just(BinaryOperator::LessThanOrEqual),
                Just(BinaryOperator::GreaterThan),
                Just(BinaryOperator::GreaterThanOrEqual),
                Just(BinaryOperator::And),
                Just(BinaryOperator::Or),
                Just(BinaryOperator::Like),
                Just(BinaryOperator::In),
                Just(BinaryOperator::Add),
                Just(BinaryOperator::Subtract),
                Just(BinaryOperator::Multiply),
                Just(BinaryOperator::Divide),
                Just(BinaryOperator::Modulo),
                Just(BinaryOperator::StringConcat),
            ]
            .boxed()
        }
    }

    impl Arbitrary for UnaryOperator {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![Just(UnaryOperator::Not), Just(UnaryOperator::Minus),].boxed()
        }
    }

    impl Arbitrary for AggregateFunc {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(AggregateFunc::Count),
                Just(AggregateFunc::Sum),
                Just(AggregateFunc::Avg),
                Just(AggregateFunc::Min),
                Just(AggregateFunc::Max),
            ]
            .boxed()
        }
    }

    impl Arbitrary for DataType {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(DataType::Int),
                Just(DataType::Float),
                Just(DataType::Text),
                Just(DataType::Boolean),
                Just(DataType::Varchar(255)),
            ]
            .boxed()
        }
    }

    impl Arbitrary for WindowFunc {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(WindowFunc::RowNumber),
                Just(WindowFunc::Rank),
                Just(WindowFunc::DenseRank),
                Just(WindowFunc::PercentRank),
            ]
            .boxed()
        }
    }

    impl Arbitrary for OrderByExpr {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            (any::<String>(), any::<bool>())
                .prop_map(|(column, ascending)| OrderByExpr { column, ascending })
                .boxed()
        }
    }

    impl Arbitrary for Expr {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            let leaf = prop_oneof![
                any::<i64>().prop_map(Expr::Number),
                any::<String>().prop_map(Expr::String),
                any::<String>().prop_map(Expr::Column),
                Just(Expr::Null),
                Just(Expr::Star),
            ];

            leaf.prop_recursive(
                4,  // max recursive depth
                10, // items per recursion level
                5,  // collection size
                |inner| {
                    prop_oneof![
                        // Binary operations
                        (inner.clone(), any::<BinaryOperator>(), inner.clone()).prop_map(
                            |(l, op, r)| Expr::BinaryOp {
                                left: Box::new(l),
                                op,
                                right: Box::new(r),
                            }
                        ),
                        // Unary operations
                        (any::<UnaryOperator>(), inner.clone())
                            .prop_map(|(op, e)| Expr::UnaryOp { op, expr: Box::new(e) }),
                        // Aliased expressions
                        (inner.clone(), any::<String>())
                            .prop_map(|(e, alias)| Expr::Alias { expr: Box::new(e), alias }),
                        // Aggregates
                        (any::<AggregateFunc>(), inner.clone())
                            .prop_map(|(func, arg)| Expr::Aggregate { func, arg: Box::new(arg) }),
                    ]
                },
            )
            .boxed()
        }
    }
}
