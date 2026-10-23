from __future__ import annotations

import fcntl
import logging
import os
import tempfile
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from pluggy2duckdb.silver_transactions_clean.schemas import (
    OUTPUT_SCHEMA,
    REQUIRED_INPUT_COLUMNS,
)

logger = logging.getLogger(__name__)

# Rank using the original updated_at type, before projecting it to VARCHAR.
# The complete source row breaks ties independently of input file order.
TRANSFORM_SQL = """
    WITH deduplicated AS (
        SELECT * FROM source
        QUALIFY row_number() OVER (
            PARTITION BY CAST(id AS VARCHAR)
            ORDER BY updated_at DESC NULLS LAST, to_json(source) DESC
        ) = 1
    )
    SELECT
        CAST(id AS VARCHAR) AS transaction_id,
        CAST(account_id AS VARCHAR) AS account_id,
        CAST(coalesce(description, description_raw) AS VARCHAR) AS description,
        CAST(currency_code AS VARCHAR) AS currency_code,
        CAST(amount AS DECIMAL(38,10)) AS amount,
        CAST(status AS VARCHAR) AS status,
        CAST(type AS VARCHAR) AS type,
        CAST(json_extract_string(TRY_CAST(payment_data AS JSON),
            '$.payer.documentNumber.value') AS VARCHAR) AS payer_document_number,
        CAST(json_extract_string(TRY_CAST(payment_data AS JSON),
            '$.payer.name') AS VARCHAR) AS payer_name,
        CAST(json_extract_string(TRY_CAST(payment_data AS JSON),
            '$.receiver.documentNumber.value') AS VARCHAR) AS receiver_document_number,
        CAST(json_extract_string(TRY_CAST(payment_data AS JSON),
            '$.receiver.name') AS VARCHAR) AS receiver_name,
        CAST(credit_card_metadata IS NOT NULL AS BOOLEAN) AS is_credit_card,
        CAST(created_at AS VARCHAR) AS created_at,
        CAST(updated_at AS VARCHAR) AS updated_at
    FROM deduplicated
"""


def _validate_ids(con, table: str, column: str) -> None:
    # Callers supply only internal table/column names.
    invalid = con.sql(
        f"SELECT count(*) FROM {table} "
        f"WHERE {column} IS NULL "
        f"OR regexp_full_match(CAST({column} AS VARCHAR), '\\s*')"
    ).fetchall()[0][0]
    if invalid:
        raise ValueError(f"{table}: {invalid} rows have empty transaction IDs")


def _write_output(relation, temporary: Path) -> None:
    with pq.ParquetWriter(temporary, OUTPUT_SCHEMA, compression="zstd") as writer:
        reader = (
            relation.to_arrow_reader(5000)
            if hasattr(relation, "to_arrow_reader")
            else relation.fetch_record_batch(5000)
        )
        for batch in reader:
            writer.write_batch(batch.cast(OUTPUT_SCHEMA))


def run_transactions_clean(
    input_path: Path,
    output_dir: Path,
    mode: str = "merge",
) -> Path:
    """Project silver transactions, atomically merging or replacing the clean file.

    Invalid IDs, incompatible schemas, and conversion/write failures abort the
    entire run. POSIX advisory locking serializes cooperating destination writers.
    """
    if mode not in {"merge", "overwrite"}:
        raise ValueError("mode must be 'merge' or 'overwrite'")
    input_path, output_dir = Path(input_path), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "transactions_clean.parquet"
    # Keep this lock file: unlinking it would allow writers to lock different inodes.
    with (output_dir / ".transactions_clean.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with duckdb.connect(":memory:") as con:
            con.execute("SET TimeZone = 'UTC'")
            schema = pq.read_schema(input_path)
            missing = REQUIRED_INPUT_COLUMNS - set(schema.names)
            if missing:
                raise ValueError(f"Missing required input columns: {', '.join(sorted(missing))}")
            if len(schema.names) != len(set(schema.names)):
                raise ValueError("Input schema contains duplicate column names")
            con.read_parquet(str(input_path)).create("source")
            _validate_ids(con, "source", "id")
            input_rows = con.sql("SELECT count(*) FROM source").fetchall()[0][0]
            con.sql(TRANSFORM_SQL).create("incoming")
            incoming_rows = con.sql("SELECT count(*) FROM incoming").fetchall()[0][0]
            existing_rows = retained_rows = 0
            query = "SELECT * FROM incoming"
            if mode == "merge" and destination.exists():
                existing_schema = pq.read_schema(destination)
                if not existing_schema.equals(OUTPUT_SCHEMA, check_metadata=False):
                    raise ValueError(
                        "Incompatible existing transactions clean schema; "
                        f"expected {OUTPUT_SCHEMA}, got {existing_schema}"
                    )
                con.read_parquet(str(destination)).create("existing")
                _validate_ids(con, "existing", "transaction_id")
                existing_rows, unique_rows = con.sql(
                    "SELECT count(*), count(DISTINCT transaction_id) FROM existing"
                ).fetchall()[0]
                if existing_rows != unique_rows:
                    raise ValueError("Existing output contains duplicate transaction IDs")
                retained = """
                    SELECT * FROM existing
                    WHERE NOT EXISTS (
                        SELECT 1 FROM incoming
                        WHERE incoming.transaction_id = existing.transaction_id
                    )
                """
                retained_rows = con.sql(f"SELECT count(*) FROM ({retained})").fetchall()[0][0]
                query += f" UNION ALL {retained}"
            relation = con.sql(f"SELECT * FROM ({query}) ORDER BY transaction_id")
            fd, filename = tempfile.mkstemp(
                prefix=".transactions_clean-", suffix=".parquet", dir=output_dir
            )
            os.close(fd)
            temporary = Path(filename)
            try:
                _write_output(relation, temporary)
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
            logger.info(
                "transactions_clean: mode=%s input_rows=%d incoming_rows=%d "
                "duplicates=%d existing_rows=%d replaced_rows=%d retained_rows=%d output_rows=%d",
                mode,
                input_rows,
                incoming_rows,
                input_rows - incoming_rows,
                existing_rows,
                existing_rows - retained_rows,
                retained_rows,
                incoming_rows + retained_rows,
            )
    return destination
