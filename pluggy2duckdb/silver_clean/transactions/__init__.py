"""Clean transaction projection and incremental Parquet merge."""

from pluggy2duckdb.silver_clean.transactions.pipeline import run_transactions_clean

__all__ = ["run_transactions_clean"]
