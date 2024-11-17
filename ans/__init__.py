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

def to_farenheit(expr: IntoExpr) -> pl.Expr:
    """Converting Celcius to Farenheit."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="to_farenheit",
        args=expr,
        is_elementwise=True,
    )

def larger(expr1: IntoExpr, expr2: IntoExpr) -> pl.Expr:
    """Return larger value."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="larger",
        args=[expr1, expr2],
        is_elementwise=True,
    )

def largest(*args: IntoExpr) -> pl.Expr:
    """Return larger value."""
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="largest",
        args=args,
        is_elementwise=True,
    )


def cum_str(expr: IntoExprColumn, sep: str, trim: bool) -> pl.Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="cum_str",
        args=[expr],
        is_elementwise=False,
        kwargs={"sep": sep, "trim": trim},
    )

def cum_str_mul(expr1: IntoExprColumn, expr2: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="cum_str_mul",
        args=[expr1, expr2],
        is_elementwise=False,
    )
