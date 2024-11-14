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
