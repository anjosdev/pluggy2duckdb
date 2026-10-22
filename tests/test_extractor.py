import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import respx
import yaml

from pluggy2duckdb.config import generate_endpoints_yaml, load_active_endpoints
from pluggy2duckdb.raw.client import PluggyClient
from pluggy2duckdb.raw.pipeline import extract_all_items
from pluggy2duckdb.silver.pipeline import run_silver
import pyarrow.parquet as pq

BASE = "https://api.pluggy.ai"


def settings(tmp_path, endpoints):
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump({"endpoints": {key: True for key in endpoints}}))
    return path


def test_generate_config_preserves_user_settings(tmp_path):
    path = settings(tmp_path, ["GET /identity"])
    before = path.read_bytes()
    generated = generate_endpoints_yaml(Path("config/oas3.json"), tmp_path / "endpoints.yaml")
    assert path.read_bytes() == before
    assert (
        yaml.safe_load(generated.read_text())["endpoints"]["GET /v2/transactions"][
            "pagination"
        ]
        == "cursor"
    )


@pytest.mark.parametrize(
    "content", ['endpoints:\n  GET /accounts: "false"', "endpoints:\n  GET /typo: true"]
)
def test_invalid_settings_rejected(tmp_path, content):
    path = tmp_path / "settings.yaml"
    path.write_text(content)
    with pytest.raises(ValueError):
        load_active_endpoints(settings_path=path)


def test_unsupported_endpoint_fails_before_authentication(tmp_path):
    path = settings(tmp_path, ["GET /merchants"])
    with pytest.raises(ValueError, match="not implemented"):
        extract_all_items(settings_path=path, output_dir=tmp_path / "raw")


def mock_item():
    respx.post(BASE + "/auth").respond(200, json={"apiKey": "test"})
    respx.get(BASE + "/items/item1").respond(
        200, json={"id": "item1", "status": "UPDATED", "executionStatus": "SUCCESS"}
    )
    respx.get(BASE + "/accounts").respond(200, json={"results": [{"id": "acc1"}]})


@respx.mock
def test_reruns_use_only_complete_latest_snapshot(tmp_path):
    mock_item()
    path = settings(tmp_path, ["GET /v2/transactions"])
    route = respx.get(BASE + "/v2/transactions")
    route.side_effect = [
        httpx.Response(
            200,
            json={"results": [{"id": "t1"}], "next": "?accountId=acc1&after=second"},
        ),
        httpx.Response(200, json={"results": [{"id": "t2"}], "next": None}),
    ]
    raw, silver = tmp_path / "raw", tmp_path / "silver"
    with PluggyClient("test", "test") as client:
        first = extract_all_items(
            settings_path=path,
            output_dir=raw,
            items_map={"bank": "item1"},
            client=client,
        )
        assert first.status == "complete"
        route.side_effect = None
        route.respond(200, json={"results": [{"id": "t3"}], "next": None})
        extract_all_items(
            settings_path=path,
            output_dir=raw,
            items_map={"bank": "item1"},
            client=client,
        )
        out = run_silver(raw, silver)
        assert pq.read_table(out["transactions"]).column("id").to_pylist() == ["t3"]
        route.respond(200, json={"results": [], "next": None})
        result = extract_all_items(
            settings_path=path,
            output_dir=raw,
            items_map={"bank": "item1"},
            client=client,
        )
        txfile = next(e["file"] for e in result.files if e["dataset"] == "transactions")
        assert (
            json.loads((raw / "runs" / result.run_id / txfile).read_text())["results"]
            == []
        )
        out = run_silver(raw, silver)
        assert pq.read_table(out["transactions"]).num_rows == 0
        before = (raw / "latest.json").read_bytes()
        route.respond(400, json={"message": "invalid"})
        with pytest.raises(httpx.HTTPStatusError):
            extract_all_items(
                settings_path=path,
                output_dir=raw,
                items_map={"bank": "item1"},
                client=client,
            )
        assert (raw / "latest.json").read_bytes() == before


@respx.mock
def test_global_endpoint_runs_once_without_items(tmp_path):
    respx.post(BASE + "/auth").respond(200, json={"apiKey": "test"})
    route = respx.get(BASE + "/categories").respond(200, json={"results": []})
    with PluggyClient("test", "test") as client:
        result = extract_all_items(
            settings_path=settings(tmp_path, ["GET /categories"]),
            output_dir=tmp_path / "raw",
            client=client,
        )
    assert route.call_count == 1
    assert result.files[0]["dataset"] == "categories"


@respx.mock
def test_stale_item_requires_explicit_opt_in(tmp_path):
    mock_item()
    respx.get(BASE + "/items/item1").respond(
        200, json={"id": "item1", "status": "UPDATING"}
    )
    kwargs = dict(
        settings_path=settings(tmp_path, ["GET /accounts"]),
        output_dir=tmp_path / "raw",
        items_map={"bank": "item1"},
    )
    with PluggyClient("test", "test") as client:
        with pytest.raises(ValueError, match="UPDATED/SUCCESS"):
            extract_all_items(**kwargs, client=client)
        assert not (tmp_path / "raw/latest.json").exists()
        result = extract_all_items(**kwargs, client=client, require_fresh=False)
        assert result.items["item1"]["status"] == "UPDATING"


@respx.mock
def test_retry_headers_and_auth_refresh():
    auth = respx.post(BASE + "/auth").respond(200, json={"apiKey": "test"})
    route = respx.get(BASE + "/accounts")
    route.side_effect = [
        httpx.Response(429, headers={"RateLimit-Reset": "40"}, json={}),
        httpx.Response(401, json={}),
        httpx.Response(200, json={"results": []}),
    ]
    with (
        patch("pluggy2duckdb.raw.client.time.sleep") as sleep,
        PluggyClient("x", "y") as client,
    ):
        assert client.fetch_single("/accounts") == {"results": []}
    sleep.assert_called_once_with(40)
    assert auth.call_count == 2


@respx.mock
def test_transport_retry():
    respx.post(BASE + "/auth").respond(200, json={"apiKey": "test"})
    respx.get(BASE + "/accounts").mock(
        side_effect=[
            httpx.ConnectError("offline"),
            httpx.Response(200, json={"results": []}),
        ]
    )
    with patch("pluggy2duckdb.raw.client.time.sleep"), PluggyClient("x", "y") as client:
        assert client.fetch_single("/accounts") == {"results": []}


@respx.mock
def test_cursor_preserves_query_and_detects_loop():
    respx.post(BASE + "/auth").respond(200, json={"apiKey": "test"})
    route = respx.get(BASE + "/v2/transactions").respond(
        200,
        json={"results": [], "next": "?accountId=a&after=x%2By%3D&dateFrom=2026-01-01"},
    )
    with PluggyClient("x", "y") as client:
        with pytest.raises(ValueError, match="repeated"):
            list(client.iter_cursor_responses("/v2/transactions", {"accountId": "a"}))
    assert route.calls[1].request.url.params["after"] == "x+y="
    assert route.calls[1].request.url.params["dateFrom"] == "2026-01-01"


@respx.mock
def test_paged_dependency_discovers_all_investments(tmp_path):
    respx.post(BASE + "/auth").respond(200, json={"apiKey": "test"})
    respx.get(BASE + "/items/item1").respond(
        200, json={"id": "item1", "status": "UPDATED", "executionStatus": "SUCCESS"}
    )

    def investments(request):
        page = int(request.url.params["page"])
        return httpx.Response(
            200,
            json={
                "page": page,
                "totalPages": 2,
                "results": [{"id": f"investment{page}"}],
            },
        )

    respx.get(BASE + "/investments").mock(side_effect=investments)
    children = [
        respx.get(BASE + f"/investments/investment{n}/transactions").respond(
            200, json={"results": [], "totalPages": 0}
        )
        for n in (1, 2)
    ]
    with PluggyClient("test", "test") as client:
        result = extract_all_items(
            settings_path=settings(tmp_path, ["GET /investments/{id}/transactions"]),
            output_dir=tmp_path / "raw",
            items_map={"bank": "item1"},
            client=client,
        )
    assert all(route.call_count == 1 for route in children)
    assert sum(entry["dataset"] == "investments" for entry in result.files) == 2
    assert result.requests
