from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EndpointSpec:
    path: str
    scope: str
    dataset: str
    pagination: str = "none"
    parent: str | None = None
    query_key: str | None = None

    @property
    def key(self) -> str:
        return f"GET {self.path}"


@dataclass
class FetchResult:
    endpoint: str
    files: list[str] = field(default_factory=list)
    rows: int = 0


@dataclass
class RunManifest:
    run_id: str
    started_at: str
    completed_at: str | None = None
    status: str = "running"
    version: int = 1
    files: list[dict[str, Any]] = field(default_factory=list)
    items: dict[str, Any] = field(default_factory=dict)
    requests: list[FetchResult] = field(default_factory=list)
