from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

import httpx


class FhirClientError(RuntimeError):
    pass


@dataclass
class FhirClient:
    base_url: str
    bearer_token: str | None
    rate_limit_qps: float
    timeout_seconds: float

    def _headers(self) -> dict[str, str]:
        h: dict[str, str] = {"Accept": "application/fhir+json"}
        if self.bearer_token:
            h["Authorization"] = f"Bearer {self.bearer_token}"
        return h

    def _sleep_for_rate_limit(self, last_ts: float | None) -> float:
        if self.rate_limit_qps <= 0:
            return time.monotonic()
        min_interval = 1.0 / self.rate_limit_qps
        now = time.monotonic()
        if last_ts is not None:
            dt = now - last_ts
            if dt < min_interval:
                time.sleep(min_interval - dt)
        return time.monotonic()

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        last_ts: float | None = None
        backoff = 0.5
        for attempt in range(6):
            last_ts = self._sleep_for_rate_limit(last_ts)
            try:
                with httpx.Client(timeout=self.timeout_seconds, headers=self._headers()) as client:
                    resp = client.get(url, params=params)
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 8)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as e:
                if attempt >= 5:
                    raise FhirClientError(f"GET {url} failed: {e}") from e
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)
        raise FhirClientError(f"GET {url} failed after retries")

    def get_capability_statement(self) -> dict[str, Any]:
        url = self.base_url.rstrip("/") + "/metadata"
        return self._get_json(url)

    def discover_resource_types(
        self,
        capability_statement: dict[str, Any],
        *,
        require_search_type: bool,
        include_resource_types: list[str],
        exclude_resource_types: list[str],
    ) -> list[str]:
        allowed = set(include_resource_types) if include_resource_types else None
        denied = set(exclude_resource_types)

        out: list[str] = []
        for rest in capability_statement.get("rest", []) or []:
            for r in rest.get("resource", []) or []:
                rtype = r.get("type")
                if not rtype:
                    continue
                if allowed is not None and rtype not in allowed:
                    continue
                if rtype in denied:
                    continue
                if require_search_type:
                    interactions = r.get("interaction", []) or []
                    if not any(i.get("code") == "search-type" for i in interactions):
                        continue
                out.append(rtype)
        return sorted(set(out))

    def sample_resources(
        self, resource_type: str, *, max_resources: int, page_size: int
    ) -> Iterator[dict[str, Any]]:
        base = self.base_url.rstrip("/")
        url = f"{base}/{resource_type}"
        params = {"_count": page_size}

        n = 0
        while url and n < max_resources:
            bundle = self._get_json(url, params=params)
            params = None  # next links already include query parameters

            for entry in bundle.get("entry", []) or []:
                res = entry.get("resource")
                if isinstance(res, dict):
                    yield res
                    n += 1
                    if n >= max_resources:
                        break

            next_url = None
            for link in bundle.get("link", []) or []:
                if link.get("relation") == "next":
                    next_url = link.get("url")
                    break
            url = next_url
