"""
Tests for Week 2 Day 1-3: Polars/Pandas Data Cleaning & Standardization.
"""

from datetime import datetime, timezone
import polars as pl
import pytest

from transformers.cleaners import (
    clean_and_normalize_strings,
    handle_null_imputations,
    normalize_emails,
    standardize_currency_amounts,
    standardize_currency_codes,
    standardize_timestamps,
)


class TestDataCleaners:
    """Tests Polars cleaning and normalization functions."""

    def test_clean_and_normalize_strings(self):
        df = pl.DataFrame({
            "name": ["  Acme Corp  ", "", "   ", "Global LLC", None],
            "city": ["New York  ", "  ", "Chicago", None, "London"],
        })
        cleaned = clean_and_normalize_strings(df)
        assert cleaned["name"].to_list() == ["Acme Corp", None, None, "Global LLC", None]
        assert cleaned["city"].to_list() == ["New York", None, "Chicago", None, "London"]

    def test_normalize_emails(self):
        df = pl.DataFrame({
            "email": [" USER@EXAMPLE.COM ", "invalid-email-address", "jane.doe@company.co.uk", None, ""]
        })
        cleaned = normalize_emails(df, email_col="email")
        assert cleaned["email"].to_list() == [
            "user@example.com",
            None,
            "jane.doe@company.co.uk",
            None,
            None,
        ]

    def test_standardize_timestamps(self):
        df = pl.DataFrame({
            "ts_unix_sec": [1704067200, 1704153600],  # 2024-01-01, 2024-01-02
            "ts_iso_str": ["2024-01-15T10:30:00.000+0000", "2024-02-01 15:45:00"],
            "ts_invalid": ["not-a-date", None],
        })
        cleaned = standardize_timestamps(
            df, timestamp_cols=["ts_unix_sec", "ts_iso_str", "ts_invalid"]
        )
        assert cleaned["ts_unix_sec"].to_list() == ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]
        assert cleaned["ts_iso_str"].to_list() == ["2024-01-15T10:30:00Z", "2024-02-01T15:45:00Z"]
        assert cleaned["ts_invalid"].to_list() == [None, None]

    def test_standardize_currency_amounts_cents(self):
        df = pl.DataFrame({
            "amount_cents": [5000, 2550, 0, None, 1299],
        })
        cleaned = standardize_currency_amounts(
            df, amount_col="amount_cents", is_in_cents=True, target_col="amount_dollars"
        )
        assert cleaned["amount_dollars"].to_list() == [50.00, 25.50, 0.00, 0.00, 12.99]

    def test_standardize_currency_codes(self):
        df = pl.DataFrame({
            "curr": ["usd", " EUR ", "gbp", "", None]
        })
        cleaned = standardize_currency_codes(df, currency_col="curr", default_currency="USD")
        assert cleaned["curr"].to_list() == ["USD", "EUR", "GBP", "USD", "USD"]

    def test_handle_null_imputations(self):
        df = pl.DataFrame({
            "balance": [10.0, None, 50.0],
            "is_active": [True, None, False],
        })
        cleaned = handle_null_imputations(df, {"balance": 0.0, "is_active": True})
        assert cleaned["balance"].to_list() == [10.0, 0.0, 50.0]
        assert cleaned["is_active"].to_list() == [True, True, False]
