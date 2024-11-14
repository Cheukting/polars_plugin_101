from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function
from polars._typing import IntoExpr, IntoExprColumn

PLUGIN_PATH = Path(__file__).parent

def capitalize(expr: IntoExpr) -> pl.Expr:
    """Capitalize String."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="capitalize",
        args=expr,
        is_elementwise=True,
    )
