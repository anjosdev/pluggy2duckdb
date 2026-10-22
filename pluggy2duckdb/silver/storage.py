"""Read completed raw runs, and publish Parquet generations through one pointer."""

import json
import logging
import os
import re
from pathlib import Path
from uuid import uuid4

from pluggy2duckdb.silver.schemas import TABLES

logger = logging.getLogger(__name__)
LINEAGE = re.compile(
    r"^(?P<item>.+?)(?:_account_(?P<account>[^_]+))?_(?:accounts|bills|v2_transactions|investments|resources|identity)(?:_page_\d+)?\.json$"
)


def _within(root: Path, name: str) -> Path:
    target = (root / name).resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError("Manifest path escapes its directory")
    return target


def raw_sources(raw_dir: Path) -> tuple[dict[str, list[dict]], str]:
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"Raw directory does not exist: {raw_dir}")
    sources = {name: [] for name in TABLES}
    latest = raw_dir / "latest.json"
    manifest_path = raw_dir / "manifest.json"
    if latest.exists():
        manifest_path = _within(raw_dir, json.loads(latest.read_text())["manifest"])
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("version") != 1 or manifest.get("status") != "complete":
            raise ValueError("Raw manifest must be a supported, complete run")
        for entry in manifest["files"]:
            path = _within(manifest_path.parent, entry["file"])
            if not path.is_file():
                raise FileNotFoundError(path)
            dataset = entry["dataset"]
            if dataset in sources:
                sources[dataset].append({**entry, "path": path})
        if not any(sources.values()):
            raise ValueError(
                "Completed raw run has no datasets covered by silver contracts"
            )
        return sources, manifest["run_id"]
    if latest.exists() or (raw_dir / "runs").exists():
        raise ValueError("No completed raw run available")
    # Migration compatibility: old flat JSON files have no completion guarantees.
    logger.warning(
        "Reading legacy flat raw files; stale pages cannot be identified without a manifest"
    )
    for table, contract in TABLES.items():
        for path in sorted(raw_dir.glob(contract["legacy_pattern"])):
            match = LINEAGE.fullmatch(path.name)
            if match is None:
                continue
            sources[table].append(
                {"path": path, "file": path.name, **match.groupdict()}
            )
    if not any(sources.values()):
        raise ValueError(
            "No recognized raw inputs; refusing to publish empty replacement"
        )
    return sources, "legacy"


def new_generation(silver_dir: Path) -> Path:
    generation = silver_dir / ".generations" / uuid4().hex
    generation.mkdir(parents=True)
    return generation


def publish(silver_dir: Path, generation: Path) -> None:
    """Serialize writers; roll back link conversion if legacy migration fails.

    Normal updates switch one symlink. The first migration from regular files
    requires a maintenance window for readers (the old files remain backed up).
    """
    import fcntl
    import shutil

    with (silver_dir / ".publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        pointer = silver_dir / f".current-{uuid4().hex}"
        pointer.symlink_to(generation.relative_to(silver_dir), target_is_directory=True)
        changed = []
        try:
            for name in TABLES:
                target = silver_dir / f"{name}.parquet"
                desired = f"current/{name}.parquet"
                if target.is_symlink() and os.readlink(target) == desired:
                    continue
                original_link = os.readlink(target) if target.is_symlink() else None
                backup = None
                if target.exists() and not target.is_symlink():
                    backup = silver_dir / ".legacy" / generation.name / target.name
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(target, backup)
                link = silver_dir / f".{name}-{uuid4().hex}"
                try:
                    link.symlink_to(desired)
                    os.replace(link, target)
                finally:
                    link.unlink(missing_ok=True)
                changed.append((target, original_link, backup))
            os.replace(pointer, silver_dir / "current")
        except Exception:
            for target, original_link, backup in reversed(changed):
                target.unlink(missing_ok=True)
                if original_link is not None:
                    target.symlink_to(original_link)
                elif backup is not None:
                    shutil.copy2(backup, target)
            raise
        finally:
            pointer.unlink(missing_ok=True)
