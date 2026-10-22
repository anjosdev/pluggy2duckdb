"""Contract projection: extras ignored, absent/invalid values become typed nulls."""

import json
import re
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext

from pluggy2duckdb.silver.schemas import TABLES


def to_snake_case(name: str) -> str:
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).replace("-", "_").lower()


def convert(value, kind):
    if value is None:
        return None
    if kind == "string":
        if not isinstance(value, str):
            raise ValueError("expected string")
        return value
    if kind == "json":
        if not isinstance(value, (dict, list)):
            raise ValueError("expected object or array")
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
            default=lambda v: float(v) if isinstance(v, Decimal) else str(v),
        )
    if kind == "bool":
        if type(value) is not bool:
            raise ValueError("expected boolean")
        return value
    if kind == "int64":
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ValueError("expected int64")
        return value
    if kind == "decimal":
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError("expected decimal")
        with localcontext() as ctx:
            ctx.prec = 50
            result = Decimal(str(value)).quantize(Decimal("0.0000000001"))
        if not result.is_finite() or result.copy_abs() >= Decimal("1e28"):
            raise ValueError("decimal out of range")
        return result
    if kind == "timestamp":
        if not isinstance(value, str):
            raise ValueError("expected ISO timestamp")
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return (
            result.replace(tzinfo=timezone.utc)
            if result.tzinfo is None
            else result.astimezone(timezone.utc)
        )
    raise ValueError(f"Unknown contract type {kind}")


def normalize(entry: dict, table: str, lineage: dict, stats: Counter) -> dict | None:
    values = {to_snake_case(k): v for k, v in entry.items()}
    contract = TABLES[table]
    stats["ignored_fields"] += len(values.keys() - contract["columns"].keys())
    # Manifest lineage is authoritative; legacy files may provide itemId themselves.
    values["item"] = lineage.get("item")
    values["item_id"] = (
        lineage.get("item_id") or values.get("item_id") or lineage.get("item")
    )
    values["account"] = lineage.get("account") or (
        values.get("id") if table == "accounts" else values.get("account_id")
    )
    values["source_file"] = lineage["file"]
    row = {}
    for name, kind in contract["columns"].items():
        try:
            row[name] = convert(values.get(name), kind)
        except (ValueError, TypeError, InvalidOperation, OverflowError):
            row[name] = None
            stats["invalid_values"] += 1
            stats[f"invalid:{name}"] += 1
    if any(row.get(key) is None or row.get(key) == "" for key in contract["keys"]):
        stats["rejected_rows"] += 1
        return None
    return row
