"""FHIR server resource-count utilities.

This module centralizes the logic for fetching a server-reported total count for
a given FHIR resource type.

It is intentionally used by both:
- verify_fhir_download.py (verification/reporting)
- download_cms_ndjson.py (pre-download expectations)

The implementation is CMS-friendly and tries multiple count-style endpoints.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Any

import httpx


__all__ = [
    "ApiCount",
    "fetch_expected_total",
]


@dataclass(frozen=True)
class ApiCount:
    resource_type: str
    expected_total: int | None
    method: str
    count_url: str | None
    curl_count_cmd: str | None
    elapsed_seconds: float | None
    attempts: int


def _build_count_url(*, client: httpx.Client, resource_type: str, params: dict[str, str]) -> str:
    """Build the exact request URL (without credentials) used to fetch counts."""

    base = client.base_url.join(resource_type)
    return str(base.copy_merge_params(params))


def _build_curl_count_cmd(*, count_url: str) -> str:
    """Build a safe, ready-to-run curl command (no embedded secrets)."""

    return f"curl -sS -u $FHIR_API_USERNAME:$FHIR_API_PASSWORD -H Accept:application/fhir+json '{count_url}'"


def _extract_total_from_bundle(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None

    # Some servers wrap a Bundle in an envelope; look for Bundle.total.
    if payload.get("resourceType") == "Bundle" and isinstance(payload.get("total"), int):
        return int(payload["total"])

    # CMS-like wrappers sometimes include "count".
    if isinstance(payload.get("count"), int):
        return int(payload["count"])

    # Look a little deeper for a nested Bundle.
    for key in ("results", "result", "bundle", "data"):
        child = payload.get(key)
        if isinstance(child, dict):
            t = _extract_total_from_bundle(child)
            if t is not None:
                return t
    return None


def _get_json_with_retries(
    *,
    client: httpx.Client,
    resource_type: str,
    params: dict[str, str],
    count_url: str,
    max_attempts: int,
    initial_timeout_seconds: float,
) -> tuple[dict[str, Any], int]:
    """GET+JSON parse with retry and exponential timeout.

    Returns: (payload_json, attempts_used)
    """

    timeout = float(initial_timeout_seconds)
    timeout_cap = float(initial_timeout_seconds)
    last_error: str | None = None

    for attempt in range(1, max(int(max_attempts), 1) + 1):
        try:
            r = client.get(resource_type, params=params, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object, got {type(payload).__name__}")
            return payload, attempt
        except Exception as ex:  # noqa: BLE001
            last_error = f"{type(ex).__name__}: {ex}"
            if attempt >= max_attempts:
                break
            next_timeout = min(timeout * 2, timeout_cap)
            print(
                f"WARN: count request failed for {resource_type} (attempt {attempt}/{max_attempts}, timeout={timeout:.0f}s) "
                f"url={count_url} error={last_error} -> retrying with timeout={next_timeout:.0f}s",
                file=sys.stderr,
            )
            timeout = next_timeout

    raise RuntimeError(
        f"count request failed for {resource_type} after {max_attempts} attempts: url={count_url} error={last_error}"
    )


def fetch_expected_total(
    *,
    client: httpx.Client,
    resource_type: str,
    max_attempts_per_url: int = 6,
    initial_timeout_seconds: float = 120.0,
) -> ApiCount:
    """Fetch expected total for a resource type using CMS-friendly methods.

    Order:
      1) ?_summary=count (standard, efficient)
      2) ?_count=0&_total=accurate (fallback)
      3) ?_count=1&_total=accurate (last resort)
    """

    attempts: list[tuple[str, dict[str, str]]] = [
        ("_summary=count", {"_summary": "count"}),
        ("_count=0&_total=accurate", {"_count": "0", "_total": "accurate"}),
        ("_count=1&_total=accurate", {"_count": "1", "_total": "accurate"}),
    ]

    last_error: str | None = None
    started = time.perf_counter()
    http_attempts = 0

    for method, params in attempts:
        try:
            count_url = _build_count_url(client=client, resource_type=resource_type, params=params)
            payload, used = _get_json_with_retries(
                client=client,
                resource_type=resource_type,
                params=params,
                count_url=count_url,
                max_attempts=int(max_attempts_per_url),
                initial_timeout_seconds=float(initial_timeout_seconds),
            )
            http_attempts += int(used)
            total = _extract_total_from_bundle(payload)
            if total is not None:
                return ApiCount(
                    resource_type=resource_type,
                    expected_total=total,
                    method=method,
                    count_url=count_url,
                    curl_count_cmd=_build_curl_count_cmd(count_url=count_url),
                    elapsed_seconds=(time.perf_counter() - started),
                    attempts=http_attempts,
                )
            last_error = f"No total found in response JSON for method={method}"
        except Exception as ex:  # noqa: BLE001
            last_error = f"{type(ex).__name__}: {ex}"
            continue

    return ApiCount(
        resource_type=resource_type,
        expected_total=None,
        method=f"FAILED: {last_error}",
        count_url=None,
        curl_count_cmd=None,
        elapsed_seconds=(time.perf_counter() - started),
        attempts=http_attempts,
    )
