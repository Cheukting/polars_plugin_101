arguments# Power up your Polars code with Polars extension

## Why polars plugin?

Polars provides a lot of build-in expressions for you to manipulate your data and DataFrames. However, sometimes our operations on DataFrames are unique and the build-in expressions does not fit the bill. You can apply a Python function to the Polars DataFrames to do that but since it is not optimised, it is slow.

In order to create user defined functions that can be compiled and registered as an expression into the Polars library, expression plugins are required. By using expression plugins, the Polars engine will dynamically link your function at runtime and your expression will run almost as fast as native expressions. Which has the benifit of:

- Optimization
- Parallelism
- Rust native performance

But there is a catch, expression plugins are available only in Rust (because of the native functionalities mentioned above) and you will need to create your function in Rust.

---

## Preflight check

In this workshop, we expect you to have knowledge of Python and Polars and have a bit of Rust experience (or be able to pick it up relatively quickly). Not all concepts in Rust will be explained but we will link to material where you can find explanations.

Here are the things that you should have installed when you started this workshop:

- [Install/ Update Rust](https://www.rust-lang.org/tools/install)(we are using rustc version 1.82.0 here)
- Make sure having Python 3.8 or above (recommend 3.12)
- Make sure using virtual environment (recommend using uv)

## Windows checklist

In this workshop we recommend using Unix OS (Mac or Linux). *If you use Windows, you may encounter problems with Rust and Maturin.* To minimise issues that you may encounter, please go through the extra checklist below:

- Install the [c++ build tools](https://visualstudio.microsoft.com/downloads/)
- [Check the `dll` files are linked correctly](https://pyo3.rs/v0.21.2/faq#im-trying-to-call-python-from-rust-but-i-get-status_dll_not_found-or-status_entrypoint_not_found)

## Learning resources for Rust and PyO3

To wirte a Polars plugin, you will have to develop in Rust. If you are not familiar with Rust, we highly recommend you first check out some of the Rust learning resources so you can be prepare for the workshop. Here are some of our recommendations:

- [The Rust Book](https://doc.rust-lang.org/book/title-page.html)
- [Rustlings (Exerciese in Rust)](https://github.com/rust-lang/rustlings)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Teach-rs (GitHub repo)](https://github.com/tweedegolf/teach-rs)

Another tool that we will be using will be PyO3 and Maturin. To learn more about them, please check out the following:

- [The PyO3 user guide](https://pyo3.rs/)
- [PyO3 101 - Writing Python modules in Rust](https://github.com/Cheukting/py03_101)

## Setting up

1. create a new working directory

```
mkdir polars-plugin-101
cd polars-plugin-101
```

2. Set up virtual environment and activate it

```
uv venv .venv
source .venv/bin/activate
python -m ensurepip --default-pip
```
*Note: the last command is needed as maturin develop cannot find pip otherwise*

3. Install **polars** and **maturin**

```
uv pip install polars maturin
```

These are the versions that we are using here:

 + maturin==1.7.4
 + polars==1.12.0

 ---

 ## Introduction: First Polars expression plugin

 Let's start by building a simple (but useful) expression. Imagine you would like to capitalise a series of names. In Python we have the build-in function to do so, but it is slow. We will try cresting something in Rust to do it.

 *Please note that this only work for ASCII characters, details please check [this discussion](https://stackoverflow.com/questions/38406793/why-is-capitalizing-the-first-letter-of-a-string-so-convoluted-in-rust)*

 ### Step 1: start a project with build settings

 Use maturin to create a project:

 ```
 maturin init
 ```

 After that, a few files will be generated for you. However, we need to customize them for a bit. In `Cargo.toml`, we would like to add a bunch of dependencies:

 ```
 polars = { version = "*" }
 pyo3 = { version = "*", features = ["extension-module", "abi3-py38"] }
 pyo3-polars = { version = "*", features = ["derive"] }
 serde = { version = "*", features = ["derive"] }
 ```

 After that, we will also need to include `polars` as our build requirements in `pyproject.toml`:

```
requires = ["maturin>=1.7,<2.0", "polars>=1.3.0"]
```

One more thing need to be added to the `pyproject.toml`, we need to make sure that the result module is named correctly so that polars can use it, add the following setting:

```
[tool.maturin]
module-name = "polars_plugin_101._internal"
```

to the `tool.maturin` session. We name our module `_internal` as it is an internal module to be used by polars.

### Step 2: define expression logic in Rust

Next, we will start developing our expression. In `src/` we have `lib.rs`, but we will create another file called `expressions.rs` to store all our custom functions.

In `src/expressions.rs`:

```rust
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
```

This is a typical structure of creating a custom polars expression. First we will have a `polars_expr` that take a `Series` as input and gives a `PolarResults`.  With the `apply_into_string_amortized` provided by `polars` we can processes each rows one by one without creating new `String` outputs.

`apply_into_string_amortized` will be calling our custom function `capitalize_str` which make a mutable borrow of the first character and make it uppercase if it is an ASCII character.

After creating our custom `polars_expr`, we can now move back to `lib.rs` to wrap up the module.

In `src/lib.rs`:

```rust
mod expressions;
use pyo3::types::PyModule;
use pyo3::{pymodule, Bound, PyResult};
use pyo3_polars::PolarsAllocator;

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();
```

For details of how to create a Python module using PyO3, please check out the other workshop [PyO3 101](https://github.com/Cheukting/py03_101/blob/main/README.md#write-our-library-code-in-rust), the difference here is that, the methods are added by `PolarsAllocator` instead of each one manually.

### Step 3: register plugin expression with Polars

To make sure Polars can find our plugin, we need to do something on the Python side. First, we will create a `__init__.py` file in the folder that matches the name of your plugin module. In our case it is `polars_plugin_101`, remember in the pyproject.toml we have:

```
[tool.maturin]
module-name = "polars_plugin_101._internal"
```

So the name before `._internal` should also matches.

Next, in the `polars_plugin_101/__init__.py` we have:

```python
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function
from polars._typing import IntoExpr

PLUGIN_PATH = Path(__file__).parent

def capitalize(expr: IntoExpr) -> pl.Expr:
    """Capitalize String."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="capitalize",
        args=expr,
        is_elementwise=True,
    )

```

This is using the plugin path, where our module created in Rust will be, to create a Polars expression. Feel free to customise the docstings, function name etc.

## Step 4: build and test the plugin

Finally, we can build and test our plugin. Again, we will use `maturin`:

```
maturin develop
```

It may take a while building for the first time, so please be patient. While waiting, why not wirte a simple Python script to test this plugin expression out?

```python
import polars as pl
from polars_plugin_101 import capitalize

df = pl.DataFrame(
    {
        "attendees": ["john", "mary", "connor", "sally"],
    }
)
out = df.with_columns(names=capitalize("attendees"))

print(out)
```

Congratulations, you have just created your first Polars plugin in Rust. Feel free to play around and test it out more.

---

## Simple numerical functions plugins

For the last exercise, we have created a expression that works on strings and create strings. Let's move one step further and create an expression that work on numerical values.

### Converting Celcius to Farenheit

Remember the formula to convert temperature in Celcius to Farenheit as:

```
deg_f = (deg_c * 9/5) + 32
```

Can we make a Polars plugin to convert a column of temperature in Celcius to Farenheit?

First, we will define the expression in `src/expressions.rs`:

```rust
#[polars_expr(output_type=Float64)]
fn to_farenheit(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Float64Chunked = s.f64()?;
    let out: Float64Chunked = ca.apply_values(|deg_c| deg_c * 9.0/5.0 + 32.0 );
    Ok(out.into_series())
}
```

Note that this time the `output_type` is `Float64` instead.

Next, we will have to register this expression with Polars, in `polars_plugin_101/__init__.py`:

```python
def to_farenheit(expr: IntoExpr) -> pl.Expr:
    """Converting Celcius to Farenheit."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="to_farenheit",
        args=expr,
        is_elementwise=True,
    )
```

This is similar to what we have done in the previous example.

Now, let's test it out by `maturin develop` and writing some Python code. Note that the correct datatype is required for the expression to work.

### Getting the larger value

Now, let's consider taking two values as inputs, they are from the same row but on a different columns. How do we go about to do that?

For example, we would like to output the larger values of the values of the two columns. In `src/expressions.rs`

```rust
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
```

Here we use `broadcast_binary_elementwise_values` provided in `polars::prelude::arity`, so make sure to include the `use` line.

When we register it with Polars in `polars_plugin_101/__init__.py`:

```python
def larger(expr1: IntoExpr, expr2: IntoExpr) -> pl.Expr:
    """Return larger value."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="larger",
        args=[expr1, expr2],
        is_elementwise=True,
    )
```

We take two arguments `expr1` and `expr2` and combine the into a list to pass in `args` to match the type `&[Series]` in our Rust input.

Now it's time to `maturin develop` and test it out! If you want an **extra challenge**, could you create an expression `largest` which take an arbitrary amount of inputs and always output the largest? ([see possible solution here](/ans))

### Expression for multiple datatypes

In the previous exercise, it assumes the data type is `Int64` and if it is another numerical data type, e.g. `Float64` it will not work and an error message will appear. You may wonder how to make a generic expression that return the larger numerical value, despite the data type (as long as it is numeric)?

Well, it will require some mapping work that need to be done while creating the expression. First, we will need to be able to return the result in the same type as the input. To do that, we will modify the marco here:

```rust
#[polars_expr(output_type=Int64)]
fn larger(inputs: &[Series]) -> PolarsResult<Series> {
...
}
```

to something like:

```rust
fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}

#[polars_expr(output_type_func=same_output_type)]
fn larger(inputs: &[Series]) -> PolarsResult<Series> {
...
}
```

The `output_type_func` argument let us [define the output type with a function](https://github.com/pola-rs/pyo3-polars/tree/a6a37eff1ac65fe8935aef960e53902f9db7f041?tab=readme-ov-file#1-shared-library-plugins-for-polars).

In our code, we assume that everything is in `i64`, now, we have to do a check on the dtype of the series and perform the corresponding code:

```rust
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
```

Note that for `f64` and `f32` there are different implementation for returning the max value.

There is no need to change the `__init__.py` so we can `maturin develop` and test it out directly. As for **extra challenge**, you can also update your `largest` method in similar fashion.

---

## Advance usage with Polars plugin

### Accumulative strings

So far we have only doing row-wise operations, what if an operation like accumulative sum or rolling average? Here we will try to perform something similar to an accumulative sum but with a twist. We will be linking up all the strings in the same column into a mega string.

In `exprssions.rs`, we deine the function:

```rust
#[polars_expr(output_type=String)]
fn cum_str(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca
        .iter()
        .scan(String::new(), |sentance: &mut String, x: Option<&str>| {
            match x {
                Some(x) => {
                    sentance.push_str(" ");
                    sentance.push_str(x);
                    Some(Some(sentance.clone()))
                },
                None => Some(None),
            }
        })
        .collect_trusted();
    Ok(out.into_series())
}
```

Note that this time we process the data as an iterator and uses the [scan](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.scan) method which holds an internal state, which we can used to store the accumulated sentence so far.

Also note that we need to return `Some(Some(sentance.clone()))` instead of `Some(Some(sentance))` to ensure the life time of the return values are long enough.

We used `collect_trusted` at the end so we need to add:

```rust
use pyo3_polars::export::polars_core::utils::CustomIterTools;
```

Next, in `__init__.py`:

```python
def cum_str(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="cum_str",
        args=[expr],
        is_elementwise=False,
    )
```

Note that we have set `is_elementwise` to `False` and the `args` takes `[expr]` as we are expecting the whole column will be used as input.

Build and try out the `cum_str` expression. For an **extra challenge**, could you create an expression that takes multiple columns as input? You can start by extend our `cum_str` to string multiple columns. ([see possible solution here](/ans))

### Taking a user argument input

In the above exercise, we separate the strings with a space, what if the user want another separator string instead of a single space? Here is how we can take an argument provide by the user, in `expressions.rs`:

```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct AddKwargs {
    sep: String,
}
```

We use a struct to hold the kwargs, we derive a `Deserialize` trait for it so we can use it later. Now we can modify our function for `cum_str`:

```rust
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
```

Adding the `kwargs: AddKwargs` to the arguments and replace the space `" "` with `&kwargs.sep`.

Remember to update in `__init__.py` as well:

```python
def cum_str(expr: IntoExprColumn, sep: str) -> pl.Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="cum_str",
        args=[expr],
        is_elementwise=False,
        kwargs={"sep": sep},
    )
```

By adding the `sep: str` in the argument and an extra `kwargs={"sep": sep}` in the `register_plugin_function` arguments.

Now you can build and try out the new `cum_str` expression with a different separator string. You may found that the separator string appears at the front of the accumulated string. This may not be desirable, as an **extra challenge**, could you add an extra argument (a flag) so the user can decide to turn on and off the leading separator strings at the front? ([see possible solution here](/ans))

---

## Support this workshop

This workshop is created by Cheuk and is open source for everyone to use (under MIT license). Please consider sponsoring Cheuk's work via [GitHub Sponsor](https://github.com/sponsors/Cheukting).
