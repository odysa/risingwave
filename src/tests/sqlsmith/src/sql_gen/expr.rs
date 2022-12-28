// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::LazyLock;

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_common::types::DataTypeName;
use risingwave_expr::expr::AggKind;
use risingwave_frontend::expr::{
    agg_func_sigs, cast_sigs, func_sigs, AggFuncSig, CastContext, CastSig, ExprType, FuncSign,
};
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName,
    TrimWhereField, UnaryOperator, Value,
};

use crate::sql_gen::utils::data_type_name_to_ast_data_type;
use crate::sql_gen::{SqlGenerator, SqlGeneratorContext};

static FUNC_TABLE: LazyLock<HashMap<DataTypeName, Vec<FuncSign>>> = LazyLock::new(|| {
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
});

static AGG_FUNC_TABLE: LazyLock<HashMap<DataTypeName, Vec<AggFuncSig>>> = LazyLock::new(|| {
    let mut funcs = HashMap::<DataTypeName, Vec<AggFuncSig>>::new();
    agg_func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
});

/// Build a cast map from return types to viable cast-signatures.
/// NOTE: We avoid cast from varchar to other datatypes apart from itself.
/// This is because arbitrary strings may not be able to cast,
/// creating large number of invalid queries.
static CAST_TABLE: LazyLock<HashMap<DataTypeName, Vec<CastSig>>> = LazyLock::new(|| {
    let mut casts = HashMap::<DataTypeName, Vec<CastSig>>::new();
    cast_sigs()
        .filter(|cast| {
            cast.context == CastContext::Explicit || cast.context == CastContext::Implicit
        })
        .filter(|cast| {
            cast.from_type != DataTypeName::Varchar || cast.to_type == DataTypeName::Varchar
        })
        .for_each(|cast| casts.entry(cast.to_type).or_default().push(cast));
    casts
});

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// In generating expression, there are two execution modes:
    /// 1) Can have Aggregate expressions (`can_agg` = true)
    ///    We can have aggregate of all bound columns (those present in GROUP BY and otherwise).
    ///    Not all GROUP BY columns need to be aggregated.
    /// 2) Can't have Aggregate expressions (`can_agg` = false)
    ///    Only columns present in GROUP BY can be selected.
    ///
    /// `inside_agg` indicates if we are calling `gen_expr` inside an aggregate.
    pub(crate) fn gen_expr(&mut self, typ: DataTypeName, context: SqlGeneratorContext) -> Expr {
        if !self.can_recurse() {
            // Stop recursion with a simple scalar or column.
            return match self.rng.gen_bool(0.5) {
                true => self.gen_simple_scalar(typ),
                false => self.gen_col(typ, context),
            };
        }

        let range = if context.can_gen_agg() { 99 } else { 90 };

        match self.rng.gen_range(0..=range) {
            0..=70 => self.gen_func(typ, context),
            71..=80 => self.gen_exists(typ, context),
            81..=90 => self.gen_cast(typ, context),
            91..=99 => self.gen_agg(typ),
            // TODO: There are more that are not in the functions table, e.g. CAST.
            // We will separately generate them.
            _ => unreachable!(),
        }
    }

    fn gen_col(&mut self, typ: DataTypeName, context: SqlGeneratorContext) -> Expr {
        let columns = if context.is_inside_agg() {
            if self.bound_relations.is_empty() {
                return self.gen_simple_scalar(typ);
            }
            self.bound_relations
                .choose(self.rng)
                .unwrap()
                .get_qualified_columns()
        } else {
            if self.bound_columns.is_empty() {
                return self.gen_simple_scalar(typ);
            }
            self.bound_columns.clone()
        };

        let matched_cols = columns
            .iter()
            .filter(|col| col.data_type == typ)
            .collect::<Vec<_>>();
        if matched_cols.is_empty() {
            self.gen_simple_scalar(typ)
        } else {
            let col_def = matched_cols.choose(&mut self.rng).unwrap();
            Expr::Identifier(Ident::new(&col_def.name))
        }
    }

    fn gen_cast(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        self.gen_cast_inner(ret, context)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generate casts from a cast map.
    /// TODO: Assign casts have to be tested via `INSERT`.
    fn gen_cast_inner(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Option<Expr> {
        let casts = CAST_TABLE.get(&ret)?;
        let cast_sig = casts.choose(&mut self.rng).unwrap();

        use CastContext as T;
        match cast_sig.context {
            T::Explicit => {
                let expr = self
                    .gen_expr(cast_sig.from_type, context.set_inside_explicit_cast())
                    .into();
                let data_type = data_type_name_to_ast_data_type(cast_sig.to_type)?;
                Some(Expr::Cast { expr, data_type })
            }

            // TODO: Re-enable implicit casts
            // Currently these implicit cast expressions may surface in:
            // select items, functions and so on.
            // Type-inference could result in different type from what SQLGenerator expects.
            // For example:
            // Suppose we had implicit cast expr from smallint->int.
            // We then generated 1::smallint with implicit type int.
            // If it was part of this expression:
            // SELECT 1::smallint as col0;
            // Then, when generating other expressions, SqlGenerator sees `col0` with type `int`,
            // but its type will be inferred as `smallint` actually in the frontend.
            //
            // Functions also encounter problems, and could infer to the wrong type.
            // May refer to type inference rules:
            // https://github.com/risingwavelabs/risingwave/blob/650810a5a9b86028036cb3b51eec5b18d8f814d5/src/frontend/src/expr/type_inference/func.rs#L445-L464
            // Therefore it is disabled for now.
            // T::Implicit if context.can_implicit_cast() => {
            //     self.gen_expr(cast_sig.from_type, context).into()
            // }

            // TODO: Generate this when e2e inserts are generated.
            // T::Assign
            _ => None,
        }
    }

    fn gen_func(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        match self.rng.gen_bool(0.1) {
            true => self.gen_variadic_func(ret, context),
            false => self.gen_fixed_func(ret, context),
        }
    }

    /// Generates functions with variable arity:
    /// `CASE`, `COALESCE`, `CONCAT`, `CONCAT_WS`
    fn gen_variadic_func(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        use DataTypeName as T;
        match ret {
            T::Varchar => match self.rng.gen_range(0..=3) {
                0 => self.gen_case(ret, context),
                1 => self.gen_coalesce(ret, context),
                2 => self.gen_concat(context),
                3 => self.gen_concat_ws(context),
                _ => unreachable!(),
            },
            _ => match self.rng.gen_bool(0.5) {
                true => self.gen_case(ret, context),
                false => self.gen_coalesce(ret, context),
            },
        }
    }

    fn gen_case(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        let n = self.rng.gen_range(1..10);
        Expr::Case {
            operand: None,
            conditions: self.gen_n_exprs_with_type(n, DataTypeName::Boolean, context),
            results: self.gen_n_exprs_with_type(n, ret, context),
            else_result: Some(Box::new(self.gen_expr(ret, context))),
        }
    }

    fn gen_coalesce(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        let non_null = self.gen_expr(ret, context);
        let position = self.rng.gen_range(0..10);
        let mut args = (0..10).map(|_| Expr::Value(Value::Null)).collect_vec();
        args[position] = non_null;
        Expr::Function(make_simple_func("coalesce", &args))
    }

    fn gen_concat(&mut self, context: SqlGeneratorContext) -> Expr {
        Expr::Function(make_simple_func("concat", &self.gen_concat_args(context)))
    }

    fn gen_concat_ws(&mut self, context: SqlGeneratorContext) -> Expr {
        let sep = self.gen_expr(DataTypeName::Varchar, context);
        let mut args = self.gen_concat_args(context);
        args.insert(0, sep);
        Expr::Function(make_simple_func("concat_ws", &args))
    }

    fn gen_concat_args(&mut self, context: SqlGeneratorContext) -> Vec<Expr> {
        let n = self.rng.gen_range(1..10);
        self.gen_n_exprs_with_type(n, DataTypeName::Varchar, context)
    }

    /// Generates `n` expressions of type `ret`.
    fn gen_n_exprs_with_type(
        &mut self,
        n: usize,
        ret: DataTypeName,
        context: SqlGeneratorContext,
    ) -> Vec<Expr> {
        (0..n).map(|_| self.gen_expr(ret, context)).collect()
    }

    fn gen_fixed_func(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        let funcs = match FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(*t, context))
            .collect();
        let expr = if exprs.len() == 1 {
            make_unary_op(func.func, &exprs[0])
        } else if exprs.len() == 2 {
            make_bin_op(func.func, &exprs)
        } else {
            None
        };
        expr.or_else(|| make_general_expr(func.func, exprs))
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    fn gen_exists(&mut self, ret: DataTypeName, context: SqlGeneratorContext) -> Expr {
        // TODO: Streaming nested loop join is not implemented yet.
        // Tracked by: <https://github.com/singularity-data/risingwave/issues/2655>.

        // Generation of subquery inside aggregation is now workaround.
        // Tracked by: <https://github.com/risingwavelabs/risingwave/issues/3896>.
        if self.is_mview || ret != DataTypeName::Boolean || context.can_gen_agg() {
            return self.gen_simple_scalar(ret);
        };
        // TODO: Feature is not yet implemented: correlated subquery in HAVING or SELECT with agg
        // let (subquery, _) = self.gen_correlated_query();
        // Tracked by: <https://github.com/risingwavelabs/risingwave/issues/2275>
        let (subquery, _) = self.gen_local_query();
        Expr::Exists(Box::new(subquery))
    }

    fn gen_agg(&mut self, ret: DataTypeName) -> Expr {
        // TODO: workaround for <https://github.com/risingwavelabs/risingwave/issues/4508>
        if ret == DataTypeName::Interval {
            return self.gen_simple_scalar(ret);
        }
        let funcs = match AGG_FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();

        let context = SqlGeneratorContext::new();
        let context = context.set_inside_agg();
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(*t, context))
            .collect();

        let distinct = self.flip_coin() && self.is_distinct_allowed;
        self.make_agg_expr(func.func, &exprs, distinct)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generates aggregate expressions. For internal / unsupported aggregators, we return `None`.
    fn make_agg_expr(&mut self, func: AggKind, exprs: &[Expr], distinct: bool) -> Option<Expr> {
        use AggKind as A;
        match func {
            A::Sum | A::Sum0 => Some(Expr::Function(make_agg_func("sum", exprs, distinct))),
            A::Min => Some(Expr::Function(make_agg_func("min", exprs, distinct))),
            A::Max => Some(Expr::Function(make_agg_func("max", exprs, distinct))),
            A::Count => Some(Expr::Function(make_agg_func("count", exprs, distinct))),
            A::Avg => Some(Expr::Function(make_agg_func("avg", exprs, distinct))),
            A::StringAgg => {
                // distinct and non_distinct_string_agg are incompatible according to
                // https://github.com/risingwavelabs/risingwave/blob/a703dc7d725aa995fecbaedc4e9569bc9f6ca5ba/src/frontend/src/optimizer/plan_node/logical_agg.rs#L394
                if self.is_distinct_allowed && !distinct {
                    None
                } else {
                    Some(Expr::Function(make_agg_func("string_agg", exprs, distinct)))
                }
            }
            A::FirstValue => None,
            A::ApproxCountDistinct => {
                if self.is_distinct_allowed {
                    None
                } else {
                    Some(Expr::Function(make_agg_func(
                        "approx_count_distinct",
                        exprs,
                        false,
                    )))
                }
            }
            // TODO(yuchao): `array_agg` support is still WIP, see #4657.
            A::ArrayAgg => None,
        }
    }
}

fn make_unary_op(func: ExprType, expr: &Expr) -> Option<Expr> {
    use {ExprType as E, UnaryOperator as U};
    let unary_op = match func {
        E::Neg => U::Minus,
        E::Not => U::Not,
        E::BitwiseNot => U::PGBitwiseNot,
        _ => return None,
    };
    Some(Expr::UnaryOp {
        op: unary_op,
        expr: Box::new(expr.clone()),
    })
}

fn make_general_expr(func: ExprType, exprs: Vec<Expr>) -> Option<Expr> {
    use ExprType as E;

    match func {
        E::Trim | E::Ltrim | E::Rtrim => Some(make_trim(func, exprs)),
        E::IsNull => Some(Expr::IsNull(Box::new(exprs[0].clone()))),
        E::IsNotNull => Some(Expr::IsNotNull(Box::new(exprs[0].clone()))),
        E::IsTrue => Some(Expr::IsTrue(Box::new(exprs[0].clone()))),
        E::IsNotTrue => Some(Expr::IsNotTrue(Box::new(exprs[0].clone()))),
        E::IsFalse => Some(Expr::IsFalse(Box::new(exprs[0].clone()))),
        E::IsNotFalse => Some(Expr::IsNotFalse(Box::new(exprs[0].clone()))),
        E::Position => Some(Expr::Function(make_simple_func("position", &exprs))),
        E::RoundDigit => Some(Expr::Function(make_simple_func("round", &exprs))),
        E::Repeat => Some(Expr::Function(make_simple_func("repeat", &exprs))),
        E::CharLength => Some(Expr::Function(make_simple_func("char_length", &exprs))),
        E::Substr => Some(Expr::Function(make_simple_func("substr", &exprs))),
        E::Length => Some(Expr::Function(make_simple_func("length", &exprs))),
        E::Upper => Some(Expr::Function(make_simple_func("upper", &exprs))),
        E::Lower => Some(Expr::Function(make_simple_func("lower", &exprs))),
        E::Replace => Some(Expr::Function(make_simple_func("replace", &exprs))),
        E::Md5 => Some(Expr::Function(make_simple_func("md5", &exprs))),
        E::ToChar => Some(Expr::Function(make_simple_func("to_char", &exprs))),
        E::SplitPart => Some(Expr::Function(make_simple_func("split_part", &exprs))),
        // TODO: Tracking issue: https://github.com/risingwavelabs/risingwave/issues/112
        // E::Translate => Some(Expr::Function(make_simple_func("translate", &exprs))),
        E::Overlay => Some(make_overlay(exprs)),
        _ => None,
    }
}

fn make_trim(func: ExprType, exprs: Vec<Expr>) -> Expr {
    use ExprType as E;

    let trim_type = match func {
        E::Trim => TrimWhereField::Both,
        E::Ltrim => TrimWhereField::Leading,
        E::Rtrim => TrimWhereField::Trailing,
        _ => unreachable!(),
    };
    let trim_where = if exprs.len() > 1 {
        Some((trim_type, Box::new(exprs[1].clone())))
    } else {
        None
    };
    Expr::Trim {
        expr: Box::new(exprs[0].clone()),
        trim_where,
    }
}

fn make_overlay(exprs: Vec<Expr>) -> Expr {
    if exprs.len() == 3 {
        Expr::Overlay {
            expr: Box::new(exprs[0].clone()),
            new_substring: Box::new(exprs[1].clone()),
            start: Box::new(exprs[2].clone()),
            count: None,
        }
    } else {
        Expr::Overlay {
            expr: Box::new(exprs[0].clone()),
            new_substring: Box::new(exprs[1].clone()),
            start: Box::new(exprs[2].clone()),
            count: Some(Box::new(exprs[3].clone())),
        }
    }
}

/// Generates simple functions such as `length`, `round`, `to_char`. These operate on datums instead
/// of columns / rows.
fn make_simple_func(func_name: &str, exprs: &[Expr]) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
    }
}

/// This is the function that generate aggregate function.
/// DISTINCT , ORDER BY or FILTER is allowed in aggregation functions。
/// Currently, distinct is allowed only, other and others rule is TODO: <https://github.com/risingwavelabs/risingwave/issues/3933>
fn make_agg_func(func_name: &str, exprs: &[Expr], distinct: bool) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct,
        order_by: vec![],
        filter: None,
    }
}

fn make_bin_op(func: ExprType, exprs: &[Expr]) -> Option<Expr> {
    use {BinaryOperator as B, ExprType as E};
    let bin_op = match func {
        E::Add => B::Plus,
        E::Subtract => B::Minus,
        E::Multiply => B::Multiply,
        E::Divide => B::Divide,
        E::Modulus => B::Modulo,
        E::GreaterThan => B::Gt,
        E::GreaterThanOrEqual => B::GtEq,
        E::LessThan => B::Lt,
        E::LessThanOrEqual => B::LtEq,
        E::Equal => B::Eq,
        E::NotEqual => B::NotEq,
        E::And => B::And,
        E::Or => B::Or,
        E::Like => B::Like,
        E::BitwiseAnd => B::BitwiseAnd,
        E::BitwiseOr => B::BitwiseOr,
        E::BitwiseXor => B::PGBitwiseXor,
        E::BitwiseShiftLeft => B::PGBitwiseShiftLeft,
        E::BitwiseShiftRight => B::PGBitwiseShiftRight,
        _ => return None,
    };
    Some(Expr::BinaryOp {
        left: Box::new(exprs[0].clone()),
        op: bin_op,
        right: Box::new(exprs[1].clone()),
    })
}

pub(crate) fn sql_null() -> Expr {
    Expr::Value(Value::Null)
}

pub fn print_function_table() -> String {
    let func_str = func_sigs()
        .map(|sign| {
            format!(
                "{:?}({}) -> {:?}",
                sign.func,
                sign.inputs_type
                    .iter()
                    .map(|arg| format!("{:?}", arg))
                    .join(", "),
                sign.ret_type,
            )
        })
        .join("\n");

    let agg_func_str = agg_func_sigs()
        .map(|sign| {
            format!(
                "{:?}({}) -> {:?}",
                sign.func,
                sign.inputs_type
                    .iter()
                    .map(|arg| format!("{:?}", arg))
                    .join(", "),
                sign.ret_type,
            )
        })
        .join("\n");

    let cast_str = cast_sigs()
        .map(|sig| {
            format!(
                "{:?} CAST {:?} -> {:?}",
                sig.context, sig.to_type, sig.from_type,
            )
        })
        .sorted()
        .join("\n");

    format!(
        "
==== FUNCTION SIGNATURES
{}

==== AGGREGATE FUNCTION SIGNATURES
{}

==== CAST SIGNATURES
{}
",
        func_str, agg_func_str, cast_str
    )
}
