import argparse
import logging
from pathlib import Path

from pluggy2duckdb.silver_clean.transactions.pipeline import run_transactions_clean


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="pluggy-silver-transactions-clean",
        description="Project silver transactions to clean Parquet",
    )
    parser.add_argument("--input", type=Path, default=Path("data/silver/transactions.parquet"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/silver_clean"))
    parser.add_argument("--mode", choices=("merge", "overwrite"), default="merge")
    args = parser.parse_args(argv)
    run_transactions_clean(args.input, args.output_dir, mode=args.mode)


if __name__ == "__main__":
    main()
