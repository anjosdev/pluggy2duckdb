"""Versioned, fixed schemas. contracts.json was bootstrapped from raw samples once.

Unknown fields never become columns automatically. Nested JSON remains opaque.
"""

import json
from importlib.resources import files

import pyarrow as pa

CONTRACT = json.loads(files(__package__).joinpath("contracts.json").read_text())
TABLES = CONTRACT["tables"]
TYPES = {
    "string": pa.string(),
    "json": pa.string(),
    "bool": pa.bool_(),
    "int64": pa.int64(),
    "decimal": pa.decimal128(38, 10),
    "timestamp": pa.timestamp("us", tz="UTC"),
}


def schema_for(table: str) -> pa.Schema:
    return pa.schema(
        [(name, TYPES[kind]) for name, kind in TABLES[table]["columns"].items()],
        metadata={b"contract_version": str(CONTRACT["version"]).encode()},
    )
