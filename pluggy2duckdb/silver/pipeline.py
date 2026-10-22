from __future__ import annotations

import json
import logging
import shutil
from collections import Counter
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pluggy2duckdb.raw.storage import atomic_json
from pluggy2duckdb.silver.schemas import CONTRACT, TABLES, schema_for
from pluggy2duckdb.silver.storage import new_generation, publish, raw_sources
from pluggy2duckdb.silver.transforms import normalize

logger = logging.getLogger(__name__)


def _project_sources(entries, table, stats):
    for source in entries:
        # Preserve decimal precision from the JSON text.
        from decimal import Decimal

        with source["path"].open(encoding="utf-8") as stream:
            payload = json.load(stream, parse_float=Decimal)
        if not isinstance(payload, dict):
            raise ValueError(f"{source['file']}: expected JSON object")
        rows = payload.get("results", [payload])
        if not isinstance(rows, list):
            raise ValueError(f"{source['file']}: expected results array")
        for entry in rows:
            stats["input_rows"] += 1
            if not isinstance(entry, dict):
                stats["rejected_rows"] += 1
                continue
            normalized = normalize(entry, table, source, stats)
            if normalized is not None:
                yield normalized


def run_silver(
    raw_dir: Path = Path("data/raw"), silver_dir: Path = Path("data/silver")
) -> dict[str, Path]:
    raw_dir, silver_dir = Path(raw_dir), Path(silver_dir)
    sources, run_id = raw_sources(raw_dir)
    generation = new_generation(silver_dir)
    report = {
        "raw_run_id": run_id,
        "contract_version": CONTRACT["version"],
        "tables": {},
    }
    try:
        with duckdb.connect(":memory:") as con:
            for table, contract in TABLES.items():
                stats = Counter()
                schema = schema_for(table)
                staging = generation / f".{table}.parquet"
                with pq.ParquetWriter(staging, schema) as writer:
                    batch = []
                    for row in _project_sources(sources[table], table, stats):
                        batch.append(row)
                        if len(batch) >= 5000:
                            writer.write_table(
                                pa.Table.from_pylist(batch, schema=schema)
                            )
                            batch.clear()
                    if batch:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                con.read_parquet(str(staging)).create_view("source", replace=True)
                keys = ", ".join(f'"{key}"' for key in contract["keys"])
                order = (
                    "updated_at DESC NULLS LAST, "
                    if "updated_at" in contract["columns"]
                    else ""
                )
                # Source + full row is a deterministic tie-breaker for equal timestamps.
                order += "source_file DESC, to_json(source) DESC"
                relation = con.sql(
                    f"SELECT * FROM source QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {order}) = 1"
                )
                output = generation / f"{table}.parquet"
                with pq.ParquetWriter(output, schema, compression="zstd") as writer:
                    reader = (
                        relation.to_arrow_reader(5000)
                        if hasattr(relation, "to_arrow_reader")
                        else relation.fetch_record_batch(5000)
                    )
                    for batch in reader:
                        writer.write_batch(batch.cast(schema))
                stats["output_rows"] = pq.read_metadata(output).num_rows
                stats["duplicates"] = (
                    stats["input_rows"] - stats["rejected_rows"] - stats["output_rows"]
                )
                staging.unlink()
                report["tables"][table] = dict(stats)
                logger.info("%s: %s", table, dict(stats))
                if (
                    stats["invalid_values"]
                    or stats["rejected_rows"]
                    or stats["ignored_fields"]
                ):
                    logger.warning(
                        "%s: contract projection ignored fields or rejected values; see report.json",
                        table,
                    )
        atomic_json(generation / "report.json", report)
        publish(silver_dir, generation)
    except Exception:
        # Keep already-published generations even if a later filesystem operation fails.
        current = silver_dir / "current"
        if not current.exists() or current.resolve() != generation.resolve():
            shutil.rmtree(generation, ignore_errors=True)
        raise
    return {table: silver_dir / f"{table}.parquet" for table in TABLES}
