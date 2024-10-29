# Power up your Polars code with Polars extension

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

 ## First Polars expression plugin

 Let's start by building a simple (but useful) expression. Imagine you would like to capitalise a series of names. In Python we have the build-in function to do so, but it is slow. We will try cresting something in Rust to do it.

 *Please note that this only work for ASCII characters, details please check [this discussion](https://stackoverflow.com/questions/38406793/why-is-capitalizing-the-first-letter-of-a-string-so-convoluted-in-rust)*

 ## Step 1: start a project with build settings

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

## Step 2: define expression logic in Rust

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

## Step 3: register plugin expression with Polars

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
maturin develop --release
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

Congratulations, you have just created your first Polars plugin in Rust.

---
