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

// std try_collect is slower than itertools
// #![feature(iterator_try_collect)]

// allow using `zip`.
// `zip_eq` is a source of poor performance.
#![allow(clippy::disallowed_methods)]

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::array::*;
use risingwave_common::types::{
    DataType, DataTypeName, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper,
    NaiveTimeWrapper, OrderedF32, OrderedF64,
};
use risingwave_expr::expr::expr_unary::new_unary_expr;
use risingwave_expr::expr::*;
use risingwave_expr::sig::agg::agg_func_sigs;
use risingwave_expr::sig::cast::cast_sigs;
use risingwave_expr::sig::func::func_sigs;
use risingwave_expr::vector_op::agg::create_agg_state_unary;
use risingwave_expr::ExprError;
use risingwave_pb::expr::expr_node::{RexNode, Type as ExprType};

criterion_group!(benches, bench_expr, bench_raw);
criterion_main!(benches);

const CHUNK_SIZE: usize = 1024;

fn bench_expr(c: &mut Criterion) {
    use itertools::Itertools;

    let input = DataChunk::new(
        vec![
            BoolArray::from_iter((1..=CHUNK_SIZE).map(|i| i % 2 == 0)).into(),
            I16Array::from_iter((1..=CHUNK_SIZE).map(|_| 1)).into(),
            I32Array::from_iter((1..=CHUNK_SIZE).map(|_| 1)).into(),
            I64Array::from_iter((1..=CHUNK_SIZE).map(|_| 1)).into(),
            F32Array::from_iter((1..=CHUNK_SIZE).map(|i| OrderedF32::from(i as f32))).into(),
            F64Array::from_iter((1..=CHUNK_SIZE).map(|i| OrderedF64::from(i as f64))).into(),
            DecimalArray::from_iter((1..=CHUNK_SIZE).map(Decimal::from)).into(),
            NaiveDateArray::from_iter((1..=CHUNK_SIZE).map(|_| NaiveDateWrapper::default())).into(),
            NaiveTimeArray::from_iter((1..=CHUNK_SIZE).map(|_| NaiveTimeWrapper::default())).into(),
            NaiveDateTimeArray::from_iter(
                (1..=CHUNK_SIZE).map(|_| NaiveDateTimeWrapper::default()),
            )
            .into(),
            I64Array::from_iter(1..=CHUNK_SIZE as i64).into(),
            IntervalArray::from_iter((1..=CHUNK_SIZE).map(|i| IntervalUnit::from_days(i as _)))
                .into(),
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(Some)).into(),
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(Some))
                .into_bytes_array()
                .into(),
            // special varchar arrays
            // 14: timezone
            Utf8Array::from_iter_display((1..=CHUNK_SIZE).map(|_| Some("Australia/Sydney"))).into(),
            // 15: time field
            Utf8Array::from_iter_display(
                [
                    "microseconds",
                    "milliseconds",
                    "second",
                    "minute",
                    "hour",
                    "day",
                    // "week",
                    "month",
                    "quarter",
                    "year",
                    "decade",
                    "century",
                    "millennium",
                ]
                .into_iter()
                .cycle()
                .take(CHUNK_SIZE)
                .map(Some),
            )
            .into(),
            // 16: extract field for date
            Utf8Array::from_iter_display(
                ["DAY", "MONTH", "YEAR", "DOW", "DOY"]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE)
                    .map(Some),
            )
            .into(),
            // 17: extract field for time
            Utf8Array::from_iter_display(
                ["HOUR", "MINUTE", "SECOND"]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE)
                    .map(Some),
            )
            .into(),
            // 18: extract field for timestamptz
            Utf8Array::from_iter_display(["EPOCH"].into_iter().cycle().take(CHUNK_SIZE).map(Some))
                .into(),
            // 19: boolean string
            Utf8Array::from_iter_display([Some(true)].into_iter().cycle().take(CHUNK_SIZE)).into(),
            // 20: date string
            Utf8Array::from_iter_display(
                [Some(NaiveDateWrapper::default())]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into(),
            // 21: time string
            Utf8Array::from_iter_display(
                [Some(NaiveTimeWrapper::default())]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into(),
            // 22: timestamp string
            Utf8Array::from_iter_display(
                [Some(NaiveDateTimeWrapper::default())]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into(),
            // 23: timestamptz string
            Utf8Array::from_iter_display(
                [Some("2021-04-01 00:00:00+00:00")]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into(),
            // 24: interval string
            Utf8Array::from_iter_display(
                [Some(IntervalUnit::default())]
                    .into_iter()
                    .cycle()
                    .take(CHUNK_SIZE),
            )
            .into(),
        ],
        CHUNK_SIZE,
    );
    let inputrefs = [
        InputRefExpression::new(DataType::Boolean, 0),
        InputRefExpression::new(DataType::Int16, 1),
        InputRefExpression::new(DataType::Int32, 2),
        InputRefExpression::new(DataType::Int64, 3),
        InputRefExpression::new(DataType::Float32, 4),
        InputRefExpression::new(DataType::Float64, 5),
        InputRefExpression::new(DataType::Decimal, 6),
        InputRefExpression::new(DataType::Date, 7),
        InputRefExpression::new(DataType::Time, 8),
        InputRefExpression::new(DataType::Timestamp, 9),
        InputRefExpression::new(DataType::Timestamptz, 10),
        InputRefExpression::new(DataType::Interval, 11),
        InputRefExpression::new(DataType::Varchar, 12),
        InputRefExpression::new(DataType::Bytea, 13),
    ];
    let inputref_for_type = |ty: DataType| {
        inputrefs
            .iter()
            .find(|r| r.return_type() == ty)
            .expect("expression not found")
    };
    const TIMEZONE: i32 = 14;
    const TIME_FIELD: i32 = 15;
    const EXTRACT_FIELD_DATE: i32 = 16;
    const EXTRACT_FIELD_TIME: i32 = 17;
    const EXTRACT_FIELD_TIMESTAMP: i32 = 16;
    const EXTRACT_FIELD_TIMESTAMPTZ: i32 = 18;
    const BOOL_STRING: i32 = 19;
    const NUMBER_STRING: i32 = 12;
    const DATE_STRING: i32 = 20;
    const TIME_STRING: i32 = 21;
    const TIMESTAMP_STRING: i32 = 22;
    const TIMESTAMPTZ_STRING: i32 = 23;
    const INTERVAL_STRING: i32 = 24;

    c.bench_function("inputref", |bencher| {
        let inputref = inputrefs[0].clone().boxed();
        bencher.iter(|| inputref.eval(&input).unwrap())
    });
    c.bench_function("constant", |bencher| {
        let constant = LiteralExpression::new(DataType::Int32, Some(1_i32.into()));
        bencher.iter(|| constant.eval(&input).unwrap())
    });

    let sigs = func_sigs();
    let sigs = sigs.sorted_by_cached_key(|sig| sig.to_string_no_return());
    for sig in sigs {
        if sig
            .inputs_type
            .iter()
            .any(|t| matches!(t, DataTypeName::Struct | DataTypeName::List))
        {
            // TODO: support struct and list
            println!("todo: {}", sig.to_string_no_return());
            continue;
        }

        let mut prost = make_expression(
            sig.func,
            &sig.inputs_type
                .iter()
                .map(|t| DataType::from(*t).prost_type_name())
                .collect_vec(),
            &sig.inputs_type
                .iter()
                .enumerate()
                .map(|(idx, t)| match (sig.func, idx) {
                    (ExprType::AtTimeZone, 1) => TIMEZONE,
                    (ExprType::DateTrunc, 0) => TIME_FIELD,
                    (ExprType::DateTrunc, 2) => TIMEZONE,
                    (ExprType::Extract, 0) => match sig.inputs_type[1] {
                        DataTypeName::Date => EXTRACT_FIELD_DATE,
                        DataTypeName::Time => EXTRACT_FIELD_TIME,
                        DataTypeName::Timestamp => EXTRACT_FIELD_TIMESTAMP,
                        DataTypeName::Timestamptz => EXTRACT_FIELD_TIMESTAMPTZ,
                        t => panic!("unexpected type: {t:?}"),
                    },
                    _ => inputref_for_type((*t).into()).index() as i32,
                })
                .collect_vec(),
        );
        if sig.func == ExprType::ToChar {
            let RexNode::FuncCall(f) = prost.rex_node.as_mut().unwrap() else { unreachable!() };
            f.children[1] = make_string_literal("YYYY/MM/DD HH:MM:SS");
        }
        let expr = match build_from_prost(&prost) {
            Ok(expr) => expr,
            Err(e) => {
                println!("error: {e}");
                continue;
            }
        };
        c.bench_function(&sig.to_string_no_return(), |bencher| {
            bencher.iter(|| expr.eval(&input).unwrap())
        });
    }

    for sig in agg_func_sigs() {
        if sig.inputs_type.len() != 1 {
            println!("todo: {}", sig.to_string_no_return());
            continue;
        }
        let mut agg = match create_agg_state_unary(
            sig.inputs_type[0].into(),
            inputref_for_type(sig.inputs_type[0].into()).index(),
            sig.func,
            sig.ret_type.into(),
            false,
        ) {
            Ok(agg) => agg,
            Err(e) => {
                println!("error: {e}");
                continue;
            }
        };
        c.bench_function(&sig.to_string_no_return(), |bencher| {
            bencher.iter(|| agg.update_multi(&input, 0, CHUNK_SIZE).unwrap())
        });
    }

    for sig in cast_sigs() {
        let expr = match new_unary_expr(
            ExprType::Cast,
            sig.to_type.into(),
            if matches!(sig.from_type, DataTypeName::Varchar) {
                use DataTypeName::*;
                let idx = match sig.to_type {
                    Boolean => BOOL_STRING,
                    Int16 | Int32 | Int64 | Float32 | Float64 | Decimal => NUMBER_STRING,
                    Date => DATE_STRING,
                    Time => TIME_STRING,
                    Timestamp => TIMESTAMP_STRING,
                    Timestamptz => TIMESTAMPTZ_STRING,
                    Interval => INTERVAL_STRING,
                    Bytea => NUMBER_STRING, // any
                    _ => {
                        println!("todo: {}", sig.to_string_no_return());
                        continue;
                    }
                };
                InputRefExpression::new(DataType::Varchar, idx as usize).boxed()
            } else {
                inputref_for_type(sig.from_type.into()).clone().boxed()
            },
        ) {
            Ok(expr) => expr,
            Err(e) => {
                println!("error: {e}");
                continue;
            }
        };
        c.bench_function(&sig.to_string_no_return(), |bencher| {
            bencher.iter(|| expr.eval(&input).unwrap())
        });
    }

    // ~360ns
    // This should be the optimization goal for our add expression.
    c.bench_function("TBD/add(int32,int32)", |bencher| {
        bencher.iter(|| {
            let a = input.column_at(2).array_ref().as_int32();
            let b = input.column_at(2).array_ref().as_int32();
            assert_eq!(a.len(), b.len());
            let mut c = (a.raw_iter())
                .zip(b.raw_iter())
                .map(|(a, b)| a + b)
                .collect::<I32Array>();
            let mut overflow = false;
            for ((a, b), c) in a.raw_iter().zip(b.raw_iter()).zip(c.raw_iter()) {
                overflow |= (c ^ a) & (c ^ b) < 0;
            }
            if overflow {
                return Err(ExprError::NumericOutOfRange);
            }
            c.set_bitmap(a.null_bitmap() & b.null_bitmap());
            Ok(c)
        })
    });
}

/// Evaluate on raw Rust array.
///
/// This could be used as a baseline to compare and tune our expressions.
fn bench_raw(c: &mut Criterion) {
    // ~55ns
    c.bench_function("raw/sum/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| a.iter().sum::<i32>())
    });
    // ~90ns
    c.bench_function("raw/add/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~600ns
    c.bench_function("raw/add/i32/zip_eq", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~950ns
    c.bench_function("raw/add/Option<i32>/zip_eq", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
    });
    enum Error {
        Overflow,
        Cast,
    }
    // ~2100ns
    c.bench_function("raw/add/Option<i32>/zip_eq,checked", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            use itertools::Itertools;
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => a.checked_add(*b).ok_or(Error::Overflow).map(Some),
                    _ => Ok(None),
                })
                .try_collect::<_, Vec<_>, Error>()
        })
    });
    // ~2400ns
    c.bench_function("raw/add/Option<i32>/zip_eq,checked,cast", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        #[allow(clippy::useless_conversion)]
        fn checked_add(a: i32, b: i32) -> Result<i32, Error> {
            let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
            let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
            a.checked_add(b).ok_or(Error::Overflow)
        }
        bencher.iter(|| {
            use itertools::Itertools;
            itertools::Itertools::zip_eq(a.iter(), b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => checked_add(*a, *b).map(Some),
                    _ => Ok(None),
                })
                .try_collect::<_, Vec<_>, Error>()
        })
    });
    // ~3100ns
    c.bench_function(
        "raw/add/Option<i32>/zip_eq,checked,cast,collect_array",
        |bencher| {
            let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
            let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
            #[allow(clippy::useless_conversion)]
            fn checked_add(a: i32, b: i32) -> Result<i32, Error> {
                let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
                let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
                a.checked_add(b).ok_or(Error::Overflow)
            }
            bencher.iter(|| {
                use itertools::Itertools;
                itertools::Itertools::zip_eq(a.iter(), b.iter())
                    .map(|(a, b)| match (a, b) {
                        (Some(a), Some(b)) => checked_add(*a, *b).map(Some),
                        _ => Ok(None),
                    })
                    .try_collect::<_, I32Array, Error>()
            })
        },
    );
}
