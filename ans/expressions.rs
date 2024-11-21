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


fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}

#[polars_expr(output_type_func=same_output_type)]
fn larger(inputs: &[Series]) -> PolarsResult<Series> {
    match inputs[0].dtype() {
        DataType::Int32 => {
            let result: Int32Chunked = broadcast_binary_elementwise_values(
                inputs[0].i32()?,
                inputs[1].i32()?,
                |first: i32, second: i32| std::cmp::max(first , second)
            );
            Ok(result.into_series())
        },
        DataType::Int64 => {
            let result: Int64Chunked = broadcast_binary_elementwise_values(
                inputs[0].i64()?,
                inputs[1].i64()?,
                |first: i64, second: i64| std::cmp::max(first , second)
            );
            Ok(result.into_series())
        },
        DataType::Float32 => {
            let result: Float32Chunked = broadcast_binary_elementwise_values(
                inputs[0].f32()?,
                inputs[1].f32()?,
                |first: f32, second: f32| f32::max(first , second)
            );
            Ok(result.into_series())
        },
        DataType::Float64 => {
            let result: Float64Chunked = broadcast_binary_elementwise_values(
                inputs[0].f64()?,
                inputs[1].f64()?,
                |first: f64, second: f64| f64::max(first , second)
            );
            Ok(result.into_series())
        },
        dtype => {
            polars_bail!(InvalidOperation:format!("dtype {dtype} not \
            supported, expected Int32, Int64, Float32 or Float64."))
        }
    }
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
    trim: bool,
}

#[polars_expr(output_type=String)]
fn cum_str(inputs: &[Series], kwargs: AddKwargs) -> PolarsResult<Series> {
    let mut start = true;
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca
        .iter()
        .scan(String::new(), |sentance: &mut String, x: Option<&str>| {
            match x {
                Some(x) => {
                    if !start | !&kwargs.trim {
                        sentance.push_str(&kwargs.sep);
                    }
                    sentance.push_str(x);
                    start = false;
                    Some(Some(sentance.clone()))
                },
                None => Some(None),
            }
        })
        .collect_trusted();
    Ok(out.into_series())
}
