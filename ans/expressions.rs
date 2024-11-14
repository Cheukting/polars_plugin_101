use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;

fn capitalize_str(value: &str, output: &mut String) {
    if let Some(mut first_char) = value.chars().next() {
        first_char.make_ascii_uppercase();
        write!(output, "{}{}", first_char, &value[1..]).unwrap()
    }
}

#[polars_expr(output_type=String)]
fn capitalize(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: StringChunked = ca.apply_into_string_amortized(capitalize_str);
    Ok(out.into_series())
}

#[polars_expr(output_type=Float64)]
fn to_farenheit(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Float64Chunked = s.f64()?;
    let out: Float64Chunked = ca.apply_values(|deg_c| deg_c * 9.0/5.0 + 32.0 );
    Ok(out.into_series())
}

use polars::prelude::arity::broadcast_binary_elementwise_values;

#[polars_expr(output_type=Int64)]
fn larger(inputs: &[Series]) -> PolarsResult<Series> {
    let first: &Int64Chunked = inputs[0].i64()?;
    let second: &Int64Chunked = inputs[1].i64()?;
    let out: Int64Chunked = broadcast_binary_elementwise_values(
        first,
        second,
        |first: i64, second: i64| std::cmp::max(first , second)
    );
    Ok(out.into_series())
}

#[polars_expr(output_type=Int64)]
fn largest(inputs: &[Series]) -> PolarsResult<Series> {
    let mut left: Int64Chunked = inputs[0].i64()?.clone();
    for input in inputs[1..].iter() {
            let right: &Int64Chunked = input.i64()?;
            left = broadcast_binary_elementwise_values(
                &left,
                right,
                |left: i64, right: i64| std::cmp::max(left , right)
            );
    }
    Ok(left.into_series())
}

use pyo3_polars::export::polars_core::utils::CustomIterTools;

// #[polars_expr(output_type=String)]
// fn cum_str(inputs: &[Series]) -> PolarsResult<Series> {
//     let s = &inputs[0];
//     let ca = s.str()?;
//     let out: StringChunked = ca
//         .iter()
//         .scan(String::new(), |sentance: &mut String, x: Option<&str>| {
//             match x {
//                 Some(x) => {
//                     sentance.push_str(" ");
//                     sentance.push_str(x);
//                     Some(Some(sentance.clone()))
//                 },
//                 None => Some(None),
//             }
//         })
//         .collect_trusted();
//     Ok(out.into_series())
// }

#[polars_expr(output_type=String)]
fn cum_str_mul(inputs: &[Series]) -> PolarsResult<Series> {
    let s1 = &inputs[0];
    let s2 = &inputs[1];
    let ca1 = s1.str()?;
    let ca2 = s2.str()?;
    let ca: StringChunked = broadcast_binary_elementwise_values(
        ca1,
        ca2,
        |ca1: &str, ca2: &str| format!("{} {}", ca1, ca2)
    );
    let out: StringChunked = ca
        .iter()
        .scan(String::new(), |sentance: &mut String, x: Option<&str>| {
            match x {
                Some(x) => {
                    sentance.push_str("\n");
                    sentance.push_str(x);
                    Some(Some(sentance.clone()))
                },
                None => Some(None),
            }
        })
        .collect_trusted();
    Ok(out.into_series())
}

use serde::Deserialize;

#[derive(Deserialize)]
struct AddKwargs {
    sep: String,
}

#[polars_expr(output_type=String)]
fn cum_str(inputs: &[Series], kwargs: AddKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca
        .iter()
        .scan(String::new(), |sentance: &mut String, x: Option<&str>| {
            match x {
                Some(x) => {
                    sentance.push_str(&kwargs.sep);
                    sentance.push_str(x);
                    Some(Some(sentance.clone()))
                },
                None => Some(None),
            }
        })
        .collect_trusted();
    Ok(out.into_series())
}
