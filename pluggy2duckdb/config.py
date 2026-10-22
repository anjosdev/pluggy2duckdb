from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

DEFAULT_BASE_URL = "https://api.pluggy.ai"
HTTP_METHODS = ("get",)


def _infer_scope(method: str, path: str, required_params: list[str]) -> str:
    if method.upper() != "GET":
        return "mutation"
    if path in ("/items/{id}", "/items/{id}/resources", "/items/{id}/scr"):
        return "item"
    if "itemId" in required_params:
        return "item"
    if path == "/connectors/{id}":
        return "connector"
    if path in (
        "/accounts/{id}",
        "/accounts/{id}/statements",
        "/accounts/{id}/balance",
    ):
        return "account"
    if "accountId" in required_params:
        return "account"
    if path in ("/investments/{id}", "/investments/{id}/transactions"):
        return "investment"
    if path == "/consents/{id}":
        return "consent"
    if path == "/identity/{id}":
        return "identity_detail"
    if path == "/loans/{id}":
        return "loan"
    if path == "/bills/{id}":
        return "bill"
    if path == "/transactions/{id}":
        return "transaction"
    if path == "/merchants":
        return "merchant"
    return "global"


def _infer_pagination(method: str, optional_params: list[str]) -> str:
    if method.upper() != "GET":
        return "none"
    if "after" in optional_params:
        return "cursor"
    if "page" in optional_params and "pageSize" in optional_params:
        return "page"
    return "none"


def generate_endpoints_yaml(
    oas_path: Path = Path("config/oas3.json"),
    endpoints_path: Path = Path("config/endpoints.yaml"),
) -> Path:
    """Generate config/endpoints.yaml from config/oas3.json."""
    with open(oas_path, "r", encoding="utf-8") as f:
        spec = json.load(f)

    endpoints: dict[str, dict[str, Any]] = {}
    for path, methods in spec.get("paths", {}).items():
        if not isinstance(methods, dict):
            continue
        for method, details in methods.items():
            if method.lower() not in HTTP_METHODS or not isinstance(details, dict):
                continue
            method_upper = method.upper()
            key = f"{method_upper} {path}"
            parameters = details.get("parameters", [])
            req_params = [
                p["name"]
                for p in parameters
                if isinstance(p, dict) and p.get("required") and "name" in p
            ]
            opt_params = [
                p["name"]
                for p in parameters
                if isinstance(p, dict) and not p.get("required") and "name" in p
            ]
            description = str(details.get("description", "") or "")
            deprecated = bool(
                details.get("deprecated", False)
                or "deprecated" in description.lower()[:60]
            )
            endpoints[key] = {
                "method": method_upper,
                "path": path,
                "summary": details.get("summary", ""),
                "scope": _infer_scope(method_upper, path, req_params),
                "pagination": _infer_pagination(method_upper, opt_params),
                "deprecated": deprecated,
                "required_params": req_params,
                "optional_params": opt_params,
            }

    endpoints_path.parent.mkdir(parents=True, exist_ok=True)
    header = "# Auto-generated from config/oas3.json.\n"
    payload = {"endpoints": endpoints}
    with open(endpoints_path, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)

    return endpoints_path


def load_active_endpoints(
    endpoints_path: Path = Path("config/endpoints.yaml"),
    settings_path: Path = Path("config/get_settings.yaml"),
    oas_path: Path = Path("config/oas3.json"),
) -> dict[str, dict[str, Any]]:
    """Read config/endpoints.yaml and config/get_settings.yaml (read-only) and return enabled endpoints."""
    if not endpoints_path.exists():
        generate_endpoints_yaml(oas_path=oas_path, endpoints_path=endpoints_path)

    with open(endpoints_path, "r", encoding="utf-8") as f:
        endpoints_doc = yaml.safe_load(f) or {}
    all_endpoints: dict[str, dict[str, Any]] = endpoints_doc.get("endpoints", {})

    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found at {settings_path}.")

    with open(settings_path, "r", encoding="utf-8") as f:
        settings_doc = yaml.safe_load(f) or {}

    toggles: dict[str, Any] = settings_doc.get("endpoints", settings_doc)
    if not isinstance(toggles, dict):
        raise ValueError("Endpoint settings must be a mapping")
    for key, enabled in toggles.items():
        if key not in all_endpoints:
            raise ValueError(f"Unknown endpoint: {key}")
        if type(enabled) is not bool:
            raise ValueError(f"{key}: expected true or false")
    active: dict[str, dict[str, Any]] = {}
    for key, meta in all_endpoints.items():
        if bool(toggles.get(key, False)):
            active[key] = meta

    return active


def load_credentials(env_path: Path | None = None) -> tuple[str, str]:
    """Load PLUGGY_CLIENT_ID and PLUGGY_CLIENT_SECRET from environment or .env."""
    if env_path is not None:
        load_dotenv(dotenv_path=env_path, override=False)
    else:
        load_dotenv(override=False)

    client_id = os.getenv("PLUGGY_CLIENT_ID", "").strip()
    client_secret = os.getenv("PLUGGY_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise ValueError(
            "PLUGGY_CLIENT_ID and PLUGGY_CLIENT_SECRET must be set in environment or .env"
        )
    return client_id, client_secret


def load_items(items_path: Path = Path("items.yaml")) -> dict[str, str]:
    """Load item aliases and UUIDs from items.yaml."""
    if not items_path.exists():
        raise FileNotFoundError(f"Items file not found: {items_path}")

    with open(items_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw:
        return {}

    if isinstance(raw, dict):
        return {
            str(alias): str(item_id).strip()
            for alias, item_id in raw.items()
            if item_id
        }
    if isinstance(raw, list):
        return {
            str(item_id).strip(): str(item_id).strip() for item_id in raw if item_id
        }

    raise ValueError(
        f"Unsupported format in {items_path}; expected mapping of alias: item_id"
    )
