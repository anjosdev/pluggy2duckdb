from __future__ import annotations

import logging
import random
import time
from typing import Any

import httpx

from pluggy2duckdb.config import DEFAULT_BASE_URL
from pluggy2duckdb.raw.pagination import iter_cursors, iter_pages

logger = logging.getLogger(__name__)


class PluggyClient:
    """Reusable backend API key, bounded retries, and strict error propagation."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1 (total attempts)")
        self.client_id, self.client_secret = client_id, client_secret
        self.max_retries = max_retries
        self._api_key: str | None = None
        self._http = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @staticmethod
    def _delay(response: httpx.Response | None, attempt: int) -> float:
        if response is not None:
            for header in ("RateLimit-Reset", "Retry-After"):
                try:
                    delay = float(response.headers.get(header, ""))
                    if 0 <= delay <= 3600:
                        return delay
                except ValueError:
                    pass
        return min(2**attempt, 60) + random.uniform(0, 0.25)

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        for attempt in range(self.max_retries):
            response = None
            try:
                response = self._http.request(method, path, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt == self.max_retries - 1:
                    raise
            else:
                if response.status_code not in (429, 500, 502, 503, 504):
                    return response
                if attempt == self.max_retries - 1:
                    response.raise_for_status()
            delay = self._delay(response, attempt)
            logger.warning("Retrying %s %s in %.2fs", method, path.split("?")[0], delay)
            time.sleep(delay)
        raise RuntimeError("Retry loop exhausted")

    def authenticate(self) -> str:
        response = self._request(
            "POST",
            "/auth",
            json={"clientId": self.client_id, "clientSecret": self.client_secret},
        )
        response.raise_for_status()
        api_key = response.json().get("apiKey")
        if not isinstance(api_key, str) or not api_key:
            raise ValueError("POST /auth response did not include apiKey")
        self._api_key = api_key
        return api_key

    def fetch_single(self, path: str, params: dict[str, Any] | None = None) -> dict:
        if self._api_key is None:
            self.authenticate()
        response = self._request(
            "GET", path, params=params, headers={"X-API-KEY": self._api_key}
        )
        if response.status_code == 401:
            self.authenticate()
            response = self._request(
                "GET", path, params=params, headers={"X-API-KEY": self._api_key}
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"{path}: expected JSON object")
        return payload

    def iter_page_responses(self, path, params=None, page_size=500):
        return iter_pages(self, path, params, page_size)

    def iter_cursor_responses(self, path, params=None):
        return iter_cursors(self, path, params)
