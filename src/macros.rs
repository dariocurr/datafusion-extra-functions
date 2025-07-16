/// Copied from, `/datafusion/functions-array/src/macros.rs`.
macro_rules! make_udaf_expr {
    ($EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        // "fluent expr_fn" style function
        #[doc = $DOC]
        pub fn $EXPR_FN(
            $($arg: datafusion::logical_expr::Expr,)*
        ) -> datafusion::logical_expr::Expr {
            datafusion::logical_expr::Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
                $AGGREGATE_UDF_FN(),
                vec![$($arg),*],
                false,
                None,
                None,
                None,
            ))
        }
    };
}

macro_rules! make_udaf_expr_and_func {
    ($UDAF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        make_udaf_expr!($EXPR_FN, $($arg)*, $DOC, $AGGREGATE_UDF_FN);
        create_func!($UDAF, $AGGREGATE_UDF_FN);
    };
    ($UDAF:ty, $EXPR_FN:ident, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        // "fluent expr_fn" style function
        #[doc = $DOC]
        pub fn $EXPR_FN(
            args: Vec<datafusion::logical_expr::Expr>,
        ) -> datafusion::logical_expr::Expr {
            datafusion::logical_expr::Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
                $AGGREGATE_UDF_FN(),
                args,
                false,
                None,
                None,
                None,
            ))
        }

        create_func!($UDAF, $AGGREGATE_UDF_FN);
    };
}

macro_rules! create_func {
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident) => {
        create_func!($UDAF, $AGGREGATE_UDF_FN, <$UDAF>::default());
    };
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident, $CREATE:expr) => {
        paste::paste! {
            /// Singleton instance of [$UDAF], ensures the UDAF is only created once
            /// named STATIC_$(UDAF). For example `STATIC_FirstValue`
            #[allow(non_upper_case_globals)]
            static [< STATIC_ $UDAF >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> =
                std::sync::OnceLock::new();

            #[doc = concat!("AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`", stringify!($UDAF), "`]")]
            pub fn $AGGREGATE_UDF_FN() -> std::sync::Arc<datafusion::logical_expr::AggregateUDF> {
                [< STATIC_ $UDAF >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::AggregateUDF::from($CREATE))
                    })
                    .clone()
            }
        }
    }
}
