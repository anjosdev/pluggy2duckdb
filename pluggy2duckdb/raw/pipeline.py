"""Dependency-aware extraction. Every dependency response is retained for replay."""

from pathlib import Path

from pluggy2duckdb.config import load_active_endpoints, load_credentials, load_items
from pluggy2duckdb.models import FetchResult, RunManifest
from pluggy2duckdb.raw.client import PluggyClient
from pluggy2duckdb.raw.endpoints import ENDPOINTS, resolve_endpoints
from pluggy2duckdb.raw.storage import RawRun


def _rows(payload):
    return payload["results"] if isinstance(payload.get("results"), list) else [payload]


def _extract(client, run, specs, item_id=None, alias=None, require_fresh=True):
    cache: dict[tuple[str, str | None], list[dict]] = {}
    parent_paths = {spec.parent for spec in specs if spec.parent}
    parent_paths.add("/items/{id}")

    def fetch(spec, entity_id=None):
        cache_key = (spec.path, entity_id)
        if cache_key in cache:
            return cache[cache_key]
        params = {spec.query_key: entity_id} if spec.query_key else None
        path = spec.path.replace("{id}", str(entity_id))
        if spec.pagination == "page":
            pages = client.iter_page_responses(path, params)
        elif spec.pagination == "cursor":
            pages = client.iter_cursor_responses(path, params)
        else:
            pages = [(1, client.fetch_single(path, params))]
        collected = []
        result = FetchResult(spec.key)
        for page, payload in pages:
            result.files.append(
                run.save(
                    payload,
                    endpoint=spec.key,
                    dataset=spec.dataset,
                    item=alias,
                    item_id=item_id,
                    account=entity_id if spec.scope == "account" else None,
                    page=page,
                )
            )
            records = _rows(payload)
            result.rows += len(records)
            # Only dependency records need to stay in memory; transaction pages stream to disk.
            if spec.path in parent_paths:
                collected.extend(records)
        run.manifest.requests.append(result)
        cache[cache_key] = collected
        return collected

    if item_id is not None:
        item = fetch(ENDPOINTS["GET /items/{id}"], item_id)[0]
        run.manifest.items[item_id] = {
            "alias": alias,
            "status": item.get("status"),
            "execution_status": item.get("executionStatus"),
            "status_detail": item.get("statusDetail"),
        }
        if require_fresh and (
            item.get("status") != "UPDATED" or item.get("executionStatus") != "SUCCESS"
        ):
            raise ValueError(
                f"Item {alias}: a complete UPDATED/SUCCESS snapshot is required; use --allow-stale explicitly to archive other states"
            )
    for spec in specs:
        if spec.scope == "global":
            fetch(spec)
        elif spec.scope == "item":
            fetch(spec, item_id)
        elif spec.scope == "connector":
            connector = item.get("connector") or {}
            if connector.get("id") is not None:
                fetch(spec, connector["id"])
        else:
            parent = ENDPOINTS[f"GET {spec.parent}"]
            for row in fetch(parent, item_id):
                if not isinstance(row, dict) or not row.get("id"):
                    raise ValueError(f"{parent.key}: missing dependency ID")
                fetch(spec, str(row["id"]))


def extract_all_items(
    items_path=Path("items.yaml"),
    output_dir=Path("data/raw"),
    endpoints_path=Path("config/endpoints.yaml"),
    settings_path=Path("config/get_settings.yaml"),
    oas_path=Path("config/oas3.json"),
    item_ids=None,
    *,
    client=None,
    require_fresh=True,
    items_map=None,
) -> RunManifest:
    specs = resolve_endpoints(
        load_active_endpoints(endpoints_path, settings_path, oas_path)
    )
    if not specs:
        raise ValueError("No endpoints enabled")
    scoped = [s for s in specs if s.scope != "global"]
    if items_map is None:
        items_map = (
            ({i: i for i in item_ids} if item_ids else load_items(items_path))
            if scoped
            else {}
        )
    if scoped and not items_map:
        raise ValueError("No items configured")
    owned = client is None
    if owned:
        client = PluggyClient(*load_credentials())
    run = RawRun(Path(output_dir))
    try:
        globals_ = [s for s in specs if s.scope == "global"]
        if globals_:
            _extract(client, run, globals_)
        if scoped:
            for alias, item_id in items_map.items():
                _extract(client, run, scoped, item_id, alias, require_fresh)
        return run.complete()
    except Exception:
        run.fail()
        raise
    finally:
        if owned:
            client.close()


def extract_item(
    item_id,
    item_key=None,
    output_dir=Path("data/raw"),
    endpoints_path=Path("config/endpoints.yaml"),
    settings_path=Path("config/get_settings.yaml"),
    oas_path=Path("config/oas3.json"),
    client=None,
    *,
    require_fresh=True,
):
    return extract_all_items(
        output_dir=output_dir,
        endpoints_path=endpoints_path,
        settings_path=settings_path,
        oas_path=oas_path,
        client=client,
        require_fresh=require_fresh,
        items_map={item_key or item_id: item_id},
    )
