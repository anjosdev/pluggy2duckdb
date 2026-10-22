import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from pluggy2duckdb.raw.storage import RawRun
from pluggy2duckdb.silver.pipeline import run_silver
from pluggy2duckdb.silver.schemas import TABLES, schema_for
from pluggy2duckdb.silver.transforms import to_snake_case


def write_run(root, datasets):
    run = RawRun(root)
    for table, rows in datasets.items():
        run.save(
            {"results": rows},
            endpoint="GET /test",
            dataset=table,
            item="bank",
            item_id="item1",
            account=None,
            page=1,
        )
    run.complete()
    return run


def test_snake_case():
    assert to_snake_case("issuerCNPJ") == "issuer_cnpj"
    assert to_snake_case("creditCardMetadata") == "credit_card_metadata"


def test_contract_ignores_unknown_fields_and_nulls_invalid_values(tmp_path):
    raw, silver = tmp_path / "raw", tmp_path / "silver's output"
    write_run(
        raw,
        {
            "accounts": [
                {
                    "id": "a",
                    "balance": "12.345",
                    "currencyCode": "BRL",
                    "newField": {"arbitrary": [1]},
                    "bankData": {"newNestedField": True},
                    "updatedAt": "2026-01-01T01:00:00-03:00",
                },
                {
                    "id": "b",
                    "balance": "invalid",
                    "currencyCode": 42,
                    "bankData": "bad",
                },
            ]
        },
    )
    output = run_silver(raw, silver)
    data = pq.read_table(output["accounts"]).to_pylist()
    rows = {r["id"]: r for r in data}
    assert "new_field" not in rows["a"]
    assert rows["a"]["balance"] == Decimal("12.345")
    assert rows["a"]["updated_at"] == datetime(2026, 1, 1, 4, tzinfo=timezone.utc)
    assert json.loads(rows["a"]["bank_data"]) == {"newNestedField": True}
    assert (
        rows["b"]["balance"] is None
        and rows["b"]["currency_code"] is None
        and rows["b"]["bank_data"] is None
    )
    assert rows["a"]["item_id"] == "item1"
    report = json.loads((silver / "current/report.json").read_text())
    assert report["tables"]["accounts"]["invalid_values"] == 3
    assert report["tables"]["accounts"]["ignored_fields"] == 1
    for table in TABLES:
        actual = pq.read_schema(output[table])
        expected = schema_for(table)
        assert actual.names == expected.names
        # DuckDB writes UTC timestamps with the equivalent Etc/UTC name.
        for field in expected:
            assert str(actual.field(field.name).type).replace("Etc/UTC", "UTC") == str(
                field.type
            )


def test_failure_preserves_previous_generation_and_unrelated_files(tmp_path):
    raw, silver = tmp_path / "raw", tmp_path / "silver"
    write_run(raw, {"accounts": [{"id": "a"}]})
    out = run_silver(raw, silver)
    before = (silver / "current").resolve()
    unrelated = silver / "other.parquet"
    unrelated.write_bytes(b"unrelated")
    run = write_run(raw, {"accounts": [{"id": "b"}]})
    (run.directory / run.manifest.files[0]["file"]).write_text("{broken")
    with pytest.raises(json.JSONDecodeError):
        run_silver(raw, silver)
    assert (silver / "current").resolve() == before
    assert pq.read_table(out["accounts"]).column("id").to_pylist() == ["a"]
    assert unrelated.read_bytes() == b"unrelated"
    with pytest.raises(FileNotFoundError):
        run_silver(tmp_path / "missing", silver)
    assert (silver / "current").resolve() == before


def test_empty_response_keeps_full_schema(tmp_path):
    raw, silver = tmp_path / "raw", tmp_path / "silver"
    write_run(raw, {"accounts": [{"id": "a"}]})
    output = run_silver(raw, silver)
    before = pq.read_schema(output["accounts"])
    write_run(raw, {"accounts": []})
    output = run_silver(raw, silver)
    assert pq.read_schema(output["accounts"]) == before
    assert pq.read_table(output["accounts"]).num_rows == 0


def test_null_keys_rejected_and_latest_duplicate_wins(tmp_path):
    raw = tmp_path / "raw"
    write_run(
        raw,
        {
            "accounts": [
                {"id": None, "name": "invalid"},
                {"id": "a", "name": "old", "updatedAt": "2026-01-01"},
                {"id": "a", "name": "new", "updatedAt": "2026-02-01"},
            ]
        },
    )
    out = run_silver(raw, tmp_path / "silver")
    rows = pq.read_table(out["accounts"]).to_pylist()
    assert len(rows) == 1 and rows[0]["name"] == "new"


def test_resource_key_scoped_to_item(tmp_path):
    raw = tmp_path / "raw"
    run = RawRun(raw)
    for item in ["item1", "item2"]:
        run.save(
            {"results": [{"resourceId": "shared", "type": "ACCOUNT"}]},
            endpoint="GET /items/{id}/resources",
            dataset="resources",
            item=item,
            item_id=item,
            account=None,
            page=1,
        )
    run.complete()
    out = run_silver(raw, tmp_path / "silver")
    assert pq.read_table(out["resources"]).num_rows == 2


def test_legacy_flat_raw_is_readable(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "my_bank_accounts.json").write_text(
        json.dumps({"results": [{"id": "a", "itemId": "real-item"}]})
    )
    out = run_silver(raw, tmp_path / "silver")
    row = pq.read_table(out["accounts"]).to_pylist()[0]
    assert row["item"] == "my_bank" and row["item_id"] == "real-item"


def test_failed_raw_run_not_consumed(tmp_path):
    raw = tmp_path / "raw"
    run = RawRun(raw)
    run.fail()
    with pytest.raises(ValueError, match="No completed"):
        run_silver(raw, tmp_path / "silver")


def test_empty_directory_does_not_publish(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    with pytest.raises(ValueError, match="No recognized"):
        run_silver(raw, tmp_path / "silver")


def test_initial_publication_failure_restores_legacy_files(tmp_path):
    import os

    raw, silver = tmp_path / "raw", tmp_path / "silver"
    write_run(raw, {"accounts": [{"id": "a"}]})
    silver.mkdir()
    original = silver / "accounts.parquet"
    original.write_bytes(b"old data")
    replace = os.replace

    def fail_pointer(src, dst):
        if Path(dst).name == "current":
            raise OSError("publication failed")
        return replace(src, dst)

    with patch("pluggy2duckdb.silver.storage.os.replace", side_effect=fail_pointer):
        with pytest.raises(OSError, match="publication failed"):
            run_silver(raw, silver)
    assert original.read_bytes() == b"old data"
    assert not original.is_symlink()
    assert not (silver / "current").exists()


def test_unrelated_raw_dataset_cannot_empty_silver(tmp_path):
    raw = tmp_path / "raw"
    write_run(raw, {"categories": []})
    with pytest.raises(ValueError, match="no datasets covered"):
        run_silver(raw, tmp_path / "silver")
