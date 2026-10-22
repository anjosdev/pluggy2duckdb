"""Pagination fails closed on malformed or repeating continuation metadata."""

from typing import Any, Iterator
from urllib.parse import urlsplit


def iter_pages(
    client, path: str, params: dict | None = None, page_size: int = 500
) -> Iterator[tuple[int, dict[str, Any]]]:
    page = 1
    while True:
        payload = client.fetch_single(
            path, {**(params or {}), "page": page, "pageSize": page_size}
        )
        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError(f"{path}: expected results array")
        yield page, payload
        total = payload.get("totalPages")
        if total is not None:
            if type(total) is not int or total < 0:
                raise ValueError(f"{path}: invalid totalPages")
            if page >= total:
                return
        elif len(results) < page_size:
            return
        page += 1


def iter_cursors(
    client, path: str, params: dict | None = None
) -> Iterator[tuple[int, dict[str, Any]]]:
    seen: set[str] = set()
    request_path, request_params = path, params
    page = 1
    while True:
        payload = client.fetch_single(request_path, request_params)
        if not isinstance(payload.get("results"), list):
            raise ValueError(f"{path}: expected results array")
        yield page, payload
        next_query = payload.get("next")
        if next_query is None:
            return
        if (
            not isinstance(next_query, str)
            or not next_query.startswith("?")
            or not urlsplit(next_query).query
        ):
            raise ValueError(f"{path}: invalid next query")
        if next_query in seen:
            raise ValueError(f"{path}: repeated pagination cursor")
        seen.add(next_query)
        # Pluggy's next is an opaque, complete query string, including filters.
        request_path, request_params = path + next_query, None
        page += 1
