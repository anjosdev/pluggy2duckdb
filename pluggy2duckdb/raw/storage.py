"""Immutable raw runs; latest.json is changed only after a complete extraction."""

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pluggy2duckdb.models import RunManifest


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class RawRun:
    def __init__(self, root: Path):
        self.root = root
        self.manifest = RunManifest(uuid4().hex, now())
        self.directory = root / "runs" / self.manifest.run_id
        self.directory.mkdir(parents=True)

    def save(
        self,
        payload: dict,
        *,
        endpoint: str,
        dataset: str,
        item: str | None,
        item_id: str | None,
        account: str | None,
        page: int,
    ) -> str:
        filename = f"{len(self.manifest.files):08d}.json"
        atomic_json(self.directory / filename, payload)
        self.manifest.files.append(
            dict(
                file=filename,
                endpoint=endpoint,
                dataset=dataset,
                item=item,
                item_id=item_id,
                account=account,
                page=page,
            )
        )
        return filename

    def complete(self) -> RunManifest:
        self.manifest.completed_at = now()
        self.manifest.status = "complete"
        atomic_json(self.directory / "manifest.json", asdict(self.manifest))
        atomic_json(
            self.root / "latest.json",
            {"manifest": f"runs/{self.manifest.run_id}/manifest.json"},
        )
        return self.manifest

    def fail(self) -> None:
        self.manifest.status = "failed"
        atomic_json(self.directory / "manifest.json", asdict(self.manifest))
