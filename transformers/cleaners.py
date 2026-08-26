"""
Data Cleaning & Standardization Module using Polars (with Pandas interoperability).
Standardizes dates to UTC ISO 8601, currency to decimal units, and handles nulls.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
import polars as pl
import pandas as pd
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


def clean_and_normalize_strings(
    df: pl.DataFrame,
    columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Trims leading/trailing whitespace and replaces empty/blank strings with None/null.
    """
    target_cols = columns or [
        col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8 or dtype == pl.String
    ]

    exprs = []
    for col in target_cols:
        if col in df.columns:
            exprs.append(
                pl.when(pl.col(col).is_not_null())
                .then(
                    pl.when(pl.col(col).str.strip_chars() == "")
                    .then(pl.lit(None, dtype=pl.String))
                    .otherwise(pl.col(col).str.strip_chars())
                )
                .otherwise(pl.lit(None, dtype=pl.String))
                .alias(col)
            )

    return df.with_columns(exprs) if exprs else df


def normalize_emails(
    df: pl.DataFrame,
    email_col: str = "email",
    target_col: Optional[str] = None,
) -> pl.DataFrame:
    """
    Normalizes email addresses to lower-case, trims whitespace,
    and sets invalid email formats to None.
    """
    if email_col not in df.columns:
        return df

    out_col = target_col or email_col

    def _clean_email(val: Optional[str]) -> Optional[str]:
        if not val or not isinstance(val, str):
            return None
        cleaned = val.strip().lower()
        if EMAIL_REGEX.match(cleaned):
            return cleaned
        return None

    # Apply vectorized mapping
    cleaned_series = pl.Series(
        out_col,
        [_clean_email(v) for v in df[email_col].to_list()],
        dtype=pl.String,
    )
    return df.with_columns(cleaned_series)


def standardize_timestamps(
    df: pl.DataFrame,
    timestamp_cols: List[str],
) -> pl.DataFrame:
    """
    Standardizes varied timestamp formats (Unix timestamps in seconds/ms,
    ISO 8601 strings with/without timezone, Salesforce format)
    into standardized UTC ISO 8601 strings and UTC Datetime.
    """
    exprs = []

    for col in timestamp_cols:
        if col not in df.columns:
            continue

        def _to_utc_iso(val: Any) -> Optional[str]:
            if val is None or val == "" or pd.isna(val):
                return None
            if isinstance(val, (int, float)):
                # If unix epoch in ms (> 1e11) vs seconds
                ts = val / 1000.0 if val > 1e11 else float(val)
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if isinstance(val, datetime):
                dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
                dt_utc = dt.astimezone(timezone.utc)
                return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            if isinstance(val, str):
                try:
                    # Strip Salesforce +0000 format or standard Z
                    parsed = date_parser.parse(val)
                    dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                    dt_utc = dt.astimezone(timezone.utc)
                    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    return None
            return None

        cleaned_vals = [_to_utc_iso(v) for v in df[col].to_list()]
        exprs.append(pl.Series(col, cleaned_vals, dtype=pl.String))

    return df.with_columns(exprs) if exprs else df


def standardize_currency_amounts(
    df: pl.DataFrame,
    amount_col: str,
    is_in_cents: bool = False,
    target_col: Optional[str] = None,
) -> pl.DataFrame:
    """
    Standardizes currency amounts to standard decimal units (e.g., Dollars),
    handling integer cents, float strings, and nulls with 2 decimal precision.
    """
    if amount_col not in df.columns:
        return df

    out_col = target_col or amount_col

    def _convert_amount(val: Any) -> float:
        if val is None or pd.isna(val):
            return 0.0
        try:
            num = float(val)
            if is_in_cents:
                num = num / 100.0
            return round(num, 2)
        except (ValueError, TypeError):
            return 0.0

    converted = [_convert_amount(v) for v in df[amount_col].to_list()]
    return df.with_columns(pl.Series(out_col, converted, dtype=pl.Float64))


def standardize_currency_codes(
    df: pl.DataFrame,
    currency_col: str = "currency",
    default_currency: str = "USD",
) -> pl.DataFrame:
    """
    Standardizes ISO currency codes to 3-letter uppercase (e.g. 'usd' -> 'USD').
    """
    if currency_col not in df.columns:
        return df.with_columns(pl.lit(default_currency).alias(currency_col))

    expr = (
        pl.when(pl.col(currency_col).is_not_null() & (pl.col(currency_col).str.strip_chars() != ""))
        .then(pl.col(currency_col).str.strip_chars().str.to_uppercase())
        .otherwise(pl.lit(default_currency))
        .alias(currency_col)
    )

    return df.with_columns(expr)


def handle_null_imputations(
    df: pl.DataFrame,
    fill_map: Dict[str, Any],
) -> pl.DataFrame:
    """
    Fills null values for designated columns with explicit typed defaults.
    """
    exprs = []
    for col, default_val in fill_map.items():
        if col in df.columns:
            exprs.append(pl.col(col).fill_null(default_val).alias(col))

    return df.with_columns(exprs) if exprs else df
