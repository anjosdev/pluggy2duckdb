import argparse
import logging
from pathlib import Path
from pluggy2duckdb.silver.pipeline import run_silver


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="pluggy-silver", description="Normalize raw JSON to silver Parquet"
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--silver-dir", type=Path, default=Path("data/silver"))
    args = parser.parse_args(argv)
    run_silver(raw_dir=args.raw_dir, silver_dir=args.silver_dir)


if __name__ == "__main__":
    main()
