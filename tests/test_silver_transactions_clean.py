import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pluggy2duckdb.silver.schemas import schema_for
from pluggy2duckdb.silver_transactions_clean import run_transactions_clean
from pluggy2duckdb.silver_transactions_clean import cli, pipeline
from pluggy2duckdb.silver_transactions_clean.schemas import OUTPUT_SCHEMA


def write_input(tmp_path, rows, schema=None, name="input.parquet"):
    path = tmp_path / name
    pq.write_table(pa.Table.from_pylist(rows, schema=schema or schema_for("transactions")), path)
    return path


def read_rows(path):
    return {row["transaction_id"]: row for row in pq.read_table(path).to_pylist()}


def test_projection_schema_precision_json_and_utc(tmp_path):
    stamp = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone(timedelta(hours=-3)))
    amount = Decimal("9999999999999999999999999999.1234567890")
    source = write_input(tmp_path, [{
        "id": "tx", "account_id": "account", "description": None,
        "description_raw": "fallback", "currency_code": "BRL", "amount": amount,
        "status": "POSTED", "type": "DEBIT", "date": stamp - timedelta(days=1),
        "created_at": stamp, "updated_at": stamp,
        "credit_card_metadata": "{}",
        "payment_data": json.dumps({
            "payer": {"name": "José", "documentNumber": {"value": "00123456789"}},
            "receiver": {"name": "Loja", "documentNumber": {"value": "00001234567890"}},
        }),
    }])
    output = run_transactions_clean(source, tmp_path / "output")
    table = pq.read_table(output)
    assert table.schema.names == [
        "transaction_id", "account_id", "description", "currency_code", "amount",
        "status", "type", "payer_document_number", "payer_name",
        "receiver_document_number", "receiver_name", "is_credit_card", "date", "created_at", "updated_at",
    ]
    assert table.schema.types == (
        [pa.string()] * 4 + [pa.decimal128(38, 10)] + [pa.string()] * 6
        + [pa.bool_()] + [pa.string()] * 3
    )
    assert table.to_pylist() == [{
        "transaction_id": "tx", "account_id": "account", "description": "fallback",
        "currency_code": "BRL", "amount": amount, "status": "POSTED", "type": "DEBIT",
        "payer_document_number": "00123456789", "payer_name": "José",
        "receiver_document_number": "00001234567890", "receiver_name": "Loja",
        "is_credit_card": True, "date": "2026-01-01 06:04:05.123456+00",
        "created_at": "2026-01-02 06:04:05.123456+00",
        "updated_at": "2026-01-02 06:04:05.123456+00",
    }]
    metadata = pq.read_metadata(output)
    assert all(metadata.row_group(0).column(i).compression == "ZSTD" for i in range(15))


@pytest.mark.parametrize("payment", [None, "", "{broken", "null", "{}", "[]", '{"payer":null}', '{"payer":{"documentNumber":{}}}'])
def test_nulls_empty_description_and_missing_or_malformed_json(tmp_path, payment):
    source = write_input(tmp_path, [
        {"id": "a", "payment_data": payment},
        {"id": "b", "description": "", "description_raw": "fallback"},
        {"id": "c", "description": "preferred", "description_raw": "fallback"},
    ])
    rows = read_rows(run_transactions_clean(source, tmp_path / "out"))
    assert rows["a"] == {
        name: "a" if name == "transaction_id" else False if name == "is_credit_card" else None
        for name in OUTPUT_SCHEMA.names
    }
    assert rows["b"]["description"] == ""
    assert rows["c"]["description"] == "preferred"


def test_explicit_casts_and_deduplication_before_string_conversion(tmp_path):
    numeric = {"id", "account_id", "description", "description_raw", "currency_code", "status", "type", "updated_at"}
    schema = pa.schema([
        (field.name, pa.int64() if field.name in numeric else field.type)
        for field in schema_for("transactions")
    ])
    rows = [
        {"id": 12, "description": 999, "updated_at": 9},
        {"id": 12, "account_id": 34, "description_raw": 56, "currency_code": 78,
         "status": 90, "type": 123, "updated_at": 10},
        {"id": 12, "description": 9999, "updated_at": None},
    ]
    source = write_input(tmp_path, rows, schema)
    row = read_rows(run_transactions_clean(source, tmp_path / "out"))["12"]
    assert {key: row[key] for key in ["account_id", "description", "currency_code", "status", "type", "updated_at"]} == {
        "account_id": "34", "description": "56", "currency_code": "78",
        "status": "90", "type": "123", "updated_at": "10",
    }


def test_latest_timestamp_and_full_row_tie_breaker_are_deterministic(tmp_path):
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new = datetime(2026, 2, 1, tzinfo=timezone.utc)
    rows = [
        {"id": "a", "updated_at": None, "description": "null"},
        {"id": "a", "updated_at": old, "description": "old"},
        {"id": "a", "updated_at": new, "description": "new"},
        {"id": "b", "updated_at": new, "description": "a"},
        {"id": "b", "updated_at": new, "description": "z"},
        {"id": "c", "description": "a"},
        {"id": "c", "description": "z"},
    ]
    source = write_input(tmp_path, rows)
    output = run_transactions_clean(source, tmp_path / "out")
    expected = pq.read_table(output)
    assert {key: row["description"] for key, row in read_rows(output).items()} == {"a": "new", "b": "z", "c": "z"}
    write_input(tmp_path, list(reversed(rows)))
    run_transactions_clean(source, output.parent, mode="overwrite")
    assert pq.read_table(output).equals(expected)


def test_create_merge_replace_retain_idempotence_and_overwrite(tmp_path, caplog):
    source = write_input(tmp_path, [
        {"id": "a", "description": "original", "amount": Decimal("12")},
        {"id": "b", "description": "retained"},
    ])
    caplog.set_level(logging.INFO, logger=pipeline.__name__)
    output = run_transactions_clean(source, tmp_path / "out")
    assert output == tmp_path / "out/transactions_clean.parquet"
    write_input(tmp_path, [{"id": "a", "description": "replacement"}, {"id": "c"}])
    run_transactions_clean(source, output.parent)
    rows = read_rows(output)
    assert set(rows) == {"a", "b", "c"}
    assert rows["a"]["description"] == "replacement" and rows["a"]["amount"] is None
    assert rows["b"]["description"] == "retained"
    assert "replaced_rows=1 retained_rows=1 output_rows=3" in caplog.text
    before = pq.read_table(output)
    run_transactions_clean(source, output.parent)
    assert pq.read_table(output).equals(before)
    run_transactions_clean(source, output.parent, mode="overwrite")
    assert set(read_rows(output)) == {"a", "c"}


@pytest.mark.parametrize("mode", ["merge", "overwrite"])
@pytest.mark.parametrize("existing", [False, True])
def test_empty_input(tmp_path, mode, existing):
    out = tmp_path / "out"
    if existing:
        run_transactions_clean(write_input(tmp_path, [{"id": "retained"}]), out)
    output = run_transactions_clean(write_input(tmp_path, []), out, mode=mode)
    table = pq.read_table(output)
    assert table.schema == OUTPUT_SCHEMA
    assert table.num_rows == (1 if existing and mode == "merge" else 0)


@pytest.mark.parametrize("bad_id", [None, "", "  ", "\t\n"])
def test_invalid_input_ids_abort_without_changing_destination(tmp_path, bad_id):
    source = write_input(tmp_path, [{"id": "original"}])
    output = run_transactions_clean(source, tmp_path / "out")
    before = output.read_bytes()
    write_input(tmp_path, [{"id": "valid"}, {"id": bad_id}])
    with pytest.raises(ValueError, match="empty transaction IDs"):
        run_transactions_clean(source, output.parent)
    assert output.read_bytes() == before


def test_missing_required_input_columns_preserves_destination(tmp_path):
    source = write_input(tmp_path, [{"id": "original"}])
    output = run_transactions_clean(source, tmp_path / "out")
    before = output.read_bytes()
    table = pq.read_table(source).drop(["payment_data", "description_raw"])
    pq.write_table(table, source)
    with pytest.raises(ValueError, match="Missing required input columns: description_raw, payment_data"):
        run_transactions_clean(source, output.parent)
    assert output.read_bytes() == before


@pytest.mark.parametrize("alteration", ["type", "order", "missing", "extra"])
def test_rejects_incompatible_existing_schema(tmp_path, alteration):
    source = write_input(tmp_path, [{"id": "original"}])
    output = run_transactions_clean(source, tmp_path / "out")
    table = pq.read_table(output)
    if alteration == "type":
        table = table.set_column(4, "amount", pa.array([None], type=pa.float64()))
    elif alteration == "order":
        table = table.select(list(reversed(table.column_names)))
    elif alteration == "missing":
        table = table.drop(["updated_at"])
    else:
        table = table.append_column("extra", pa.array(["extra"]))
    pq.write_table(table, output)
    before = output.read_bytes()
    with pytest.raises(ValueError, match="Incompatible existing"):
        run_transactions_clean(source, output.parent)
    assert output.read_bytes() == before
    # Overwrite is also a way to deliberately rebuild an incompatible destination.
    run_transactions_clean(source, output.parent, mode="overwrite")
    assert pq.read_schema(output) == OUTPUT_SCHEMA


@pytest.mark.parametrize("ids", [["duplicate", "duplicate"], [None], [""]])
def test_existing_ids_must_be_unique_and_nonempty(tmp_path, ids):
    source = write_input(tmp_path, [{"id": "original"}])
    output = run_transactions_clean(source, tmp_path / "out")
    pq.write_table(pa.Table.from_pylist([{"transaction_id": id_} for id_ in ids], schema=OUTPUT_SCHEMA), output)
    before = output.read_bytes()
    with pytest.raises(ValueError, match="transaction IDs"):
        run_transactions_clean(source, output.parent)
    assert output.read_bytes() == before


@pytest.mark.parametrize("failure", ["write", "replace", "amount", "missing_input"])
def test_failures_preserve_output_and_remove_temporary_files(tmp_path, failure):
    source = write_input(tmp_path, [{"id": "original"}])
    output = run_transactions_clean(source, tmp_path / "out")
    before = output.read_bytes()
    unrelated = output.parent / "unrelated.txt"
    unrelated.write_text("keep")
    write_input(tmp_path, [{"id": "replacement"}])
    if failure == "write":
        def fail_write(relation, temporary):
            temporary.write_bytes(b"partial")
            raise OSError("write failed")
        with patch.object(pipeline, "_write_output", side_effect=fail_write):
            with pytest.raises(OSError, match="write failed"):
                run_transactions_clean(source, output.parent)
    elif failure == "replace":
        with patch.object(pipeline.os, "replace", side_effect=OSError("replace failed")):
            with pytest.raises(OSError, match="replace failed"):
                run_transactions_clean(source, output.parent)
    elif failure == "amount":
        schema = pa.schema([(f.name, pa.string() if f.name == "amount" else f.type) for f in schema_for("transactions")])
        write_input(tmp_path, [{"id": "replacement", "amount": "invalid"}], schema)
        with pytest.raises(pipeline.duckdb.ConversionException):
            run_transactions_clean(source, output.parent)
    else:
        source.unlink()
        with pytest.raises(FileNotFoundError):
            run_transactions_clean(source, output.parent)
    assert output.read_bytes() == before
    assert unrelated.read_text() == "keep"
    assert not list(output.parent.glob(".transactions_clean-*.parquet"))


def test_writers_serialize_before_reading_existing_output(tmp_path):
    output_dir = tmp_path / "out"
    source = write_input(tmp_path, [{"id": "original"}])
    output = run_transactions_clean(source, output_dir)
    first = write_input(tmp_path, [{"id": "first"}], name="first.parquet")
    second = write_input(tmp_path, [{"id": "second"}], name="second.parquet")
    writing, release, second_attempt, second_locked = Event(), Event(), Event(), Event()
    write_output, flock = pipeline._write_output, pipeline.fcntl.flock
    calls = 0

    def observed_lock(*args):
        nonlocal calls
        calls += 1
        is_second = calls == 2
        if is_second:
            second_attempt.set()
        result = flock(*args)
        if is_second:
            second_locked.set()
        return result

    def paused_write(relation, temporary):
        if not writing.is_set():
            writing.set()
            assert release.wait(10)
        write_output(relation, temporary)

    with patch.object(pipeline, "_write_output", side_effect=paused_write), patch.object(pipeline.fcntl, "flock", side_effect=observed_lock):
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(run_transactions_clean, first, output_dir)
            try:
                assert writing.wait(10)
                b = pool.submit(run_transactions_clean, second, output_dir)
                assert second_attempt.wait(10)
                assert not second_locked.wait(0.15)
            finally:
                release.set()
            a.result(timeout=10)
            b.result(timeout=10)
    assert set(read_rows(output)) == {"original", "first", "second"}


def test_cli_defaults_and_options():
    with patch.object(cli, "run_transactions_clean") as run:
        cli.main([])
        run.assert_called_once_with(Path("data/silver/transactions.parquet"), Path("data/silver_clean"), mode="merge")
    with patch.object(cli, "run_transactions_clean") as run:
        cli.main(["--input", "in.parquet", "--output-dir", "out", "--mode", "overwrite"])
        run.assert_called_once_with(Path("in.parquet"), Path("out"), mode="overwrite")
    with pytest.raises(SystemExit) as exc:
        cli.main(["--mode", "invalid"])
    assert exc.value.code == 2


def test_invalid_mode_does_not_create_destination(tmp_path):
    with pytest.raises(ValueError, match="mode must be"):
        run_transactions_clean(tmp_path / "missing", tmp_path / "out", mode="invalid")
    assert not (tmp_path / "out").exists()
