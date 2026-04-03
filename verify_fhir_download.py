#!/usr/bin/env python3
"""Verify that a directory of downloaded NDJSON matches what a FHIR server reports.

Goal
----
Given an output directory produced by our downloader (e.g. `create_ndjson_from_api.py`),
quickly answer:

* How many lines are in each NDJSON file?
* How many *unique* FHIR ids are present (detect duplicates)?
* Does that match the server-reported total count for that resource type?

This script is intentionally read-only:
* It reads authentication credentials from `.env` via the project's env loader.
* It does NOT print credentials.

Usage
-----

    python verify_fhir_download.py /path/to/ndjson_dir https://dev.cnpd.internal.cms.gov/fhir/

Exit codes
----------
0: all checked resource types match within allowed delta and no parse errors
2: any mismatch, duplicates, parse errors, or API count errors
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import httpx


def _ensure_src_on_path() -> None:
    """Allow running this repo-root script without installation."""

    repo_root = Path(__file__).resolve().parent
    src = repo_root / "src"
    if src.exists() and str(src) not in sys.path:
        sys.path.insert(0, str(src))


_ensure_src_on_path()

from fhir_tablesaw_3tier.env import get_fhir_basic_auth, load_dotenv  # noqa: E402


@dataclass(frozen=True)
class NdjsonStats:
    resource_type: str
    path: Path
    line_count: int
    unique_id_count: int
    duplicate_id_count: int
    parse_error_count: int
    missing_id_count: int


@dataclass(frozen=True)
class ApiCount:
    resource_type: str
    expected_total: int | None
    method: str
    count_url: str | None
    curl_count_cmd: str | None
    elapsed_seconds: float | None
    attempts: int


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

    Requirements:
    - Retry each URL up to `max_attempts` times.
    - First attempt uses `initial_timeout_seconds`.
    - Timeout doubles after every failed attempt.

    Returns:
      (payload_json, attempts_used)
    """

    timeout = float(initial_timeout_seconds)
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
            next_timeout = timeout * 2
            print(
                f"WARN: count request failed for {resource_type} (attempt {attempt}/{max_attempts}, timeout={timeout:.0f}s) "
                f"url={count_url} error={last_error} -> retrying with timeout={next_timeout:.0f}s",
                file=sys.stderr,
            )
            timeout = next_timeout

    raise RuntimeError(
        f"count request failed for {resource_type} after {max_attempts} attempts: url={count_url} error={last_error}"
    )


def _build_count_url(*, client: httpx.Client, resource_type: str, params: dict[str, str]) -> str:
    """Build the exact request URL (without credentials) used to fetch counts."""

    # httpx.URL join keeps base path (e.g. /fhir/) and appends resource_type.
    base = client.base_url.join(resource_type)
    # Merge query params.
    return str(base.copy_merge_params(params))


def _build_curl_count_cmd(*, count_url: str) -> str:
    """Build a safe, ready-to-run curl command (no embedded secrets).

    NOTE: We intentionally do NOT embed the real username/password.
    """

    # Keep the command easy to paste, while avoiding embedded quotes that get messy in CSV.
    # We still do NOT embed real credentials.
    return f"curl -sS -u $FHIR_API_USERNAME:$FHIR_API_PASSWORD -H Accept:application/fhir+json '{count_url}'"


def _resource_type_from_filename(p: Path) -> str:
    """Infer resource type from snake_case file name.

    Example: location.ndjson -> Location, organization_affiliation.ndjson -> OrganizationAffiliation
    """

    stem = p.name
    if stem.endswith(".ndjson"):
        stem = stem[: -len(".ndjson")]
    parts = [x for x in stem.split("_") if x]
    if not parts:
        return "Unknown"
    return "".join([part[:1].upper() + part[1:] for part in parts])


def _iter_ndjson_files(ndjson_dir: Path) -> Iterable[Path]:
    # Prefer top-level *.ndjson files (what our downloader creates).
    yield from sorted(ndjson_dir.glob("*.ndjson"))


def compute_ndjson_stats(*, ndjson_path: Path, resource_type: str) -> NdjsonStats:
    line_count = 0
    parse_error_count = 0
    missing_id_count = 0
    ids: set[str] = set()
    duplicate_ids = 0

    # Stream line by line to handle huge files.
    with ndjson_path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            line_count += 1
            try:
                obj = json.loads(line)
            except Exception:
                parse_error_count += 1
                continue

            # Verify it looks like a FHIR resource of the expected type.
            # If the resourceType doesn't match the file we're scanning, treat it like
            # a parse/integrity error (it means the download directory isn't internally
            # consistent).
            if not _resource_type_matches(resource_type=resource_type, obj=obj):
                parse_error_count += 1
                continue

            # We count missing ids as an error signal but still allow the script to continue.
            rid = obj.get("id") if isinstance(obj, dict) else None
            if not rid or not isinstance(rid, str):
                missing_id_count += 1
                continue

            if rid in ids:
                duplicate_ids += 1
            else:
                ids.add(rid)

    return NdjsonStats(
        resource_type=resource_type,
        path=ndjson_path,
        line_count=line_count,
        unique_id_count=len(ids),
        duplicate_id_count=duplicate_ids,
        parse_error_count=parse_error_count,
        missing_id_count=missing_id_count,
    )


def _resource_type_matches(*, resource_type: str, obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    rt = obj.get("resourceType")
    return isinstance(rt, str) and rt == resource_type


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
        # Some servers accept either 'accurate' or 'none'. For CMS we want a real total.
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

    # We intentionally return expected_total=None so the caller can still run
    # integrity/dup checks but will mark overall status as FAIL.
    return ApiCount(
        resource_type=resource_type,
        expected_total=None,
        method=f"FAILED: {last_error}",
        count_url=None,
        curl_count_cmd=None,
        elapsed_seconds=(time.perf_counter() - started),
        attempts=http_attempts,
    )


def _fmt_int(x: int | None) -> str:
    return "?" if x is None else str(int(x))


def write_csv_report(
    *,
    csv_out: Path,
    pairs: list[tuple[str, Path]],
    client: httpx.Client,
    allow_delta: int,
    api_max_attempts_per_url: int,
    api_initial_timeout_seconds: float,
) -> bool:
    """Write the verification report and return True if any row is FAIL."""

    any_fail = False

    # Minimal CSV (as requested): keep only the core comparison numbers.
    fieldnames = [
        "fhir_resource_type",
        "resource_id_count_from_file",
        "resource_id_count_from_url",
    ]

    csv_out.parent.mkdir(parents=True, exist_ok=True)
    with csv_out.open("w", newline="", encoding="utf-8") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()

        for rt, fpath in pairs:
            print(f"Working on {rt}...", file=sys.stderr)

            t0 = time.perf_counter()
            stats = compute_ndjson_stats(ndjson_path=fpath, resource_type=rt)
            file_elapsed = time.perf_counter() - t0

            t1 = time.perf_counter()
            api = fetch_expected_total(
                client=client,
                resource_type=rt,
                max_attempts_per_url=int(api_max_attempts_per_url),
                initial_timeout_seconds=float(api_initial_timeout_seconds),
            )
            api_elapsed = time.perf_counter() - t1

            # Decide PASS/FAIL.
            status = "PASS"
            if stats.parse_error_count or stats.missing_id_count or stats.duplicate_id_count:
                status = "FAIL"
            if api.expected_total is None:
                status = "FAIL"
            else:
                delta = abs(int(api.expected_total) - int(stats.unique_id_count))
                if delta > int(allow_delta):
                    status = "FAIL"

            # Always emit a machine-readable status summary to stderr.
            # Keep stdout for the CSV path message.
            if api.expected_total is None:
                delta_val: int | None = None
            else:
                delta_val = abs(int(api.expected_total) - int(stats.unique_id_count))
            print(
                "VERIFY_STATUS "
                f"resource_type={rt} status={status} "
                f"file_unique_ids={stats.unique_id_count} "
                f"api_total={_fmt_int(api.expected_total)} "
                f"delta={_fmt_int(delta_val)} "
                f"parse_errors={stats.parse_error_count} missing_ids={stats.missing_id_count} duplicates={stats.duplicate_id_count}",
                file=sys.stderr,
            )

            if status != "PASS":
                any_fail = True

            writer.writerow(
                {
                    "fhir_resource_type": rt,
                    "resource_id_count_from_file": stats.unique_id_count,
                    "resource_id_count_from_url": api.expected_total if api.expected_total is not None else "",
                }
            )

            api_method = api.method
            print(
                f"Done {rt}. timings: file_parse_seconds={file_elapsed:.3f} "
                f"api_count_seconds={api_elapsed:.3f} api_attempts={api.attempts} api_method={api_method}",
                file=sys.stderr,
            )

    return any_fail


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="verify_fhir_download.py",
        description="Verify NDJSON downloads against server-reported FHIR counts",
    )
    p.add_argument("ndjson_dir", help="Directory containing *.ndjson files")
    p.add_argument("fhir_url", help="FHIR base URL (e.g. https://.../fhir/)")
    p.add_argument(
        "--allow-delta",
        type=int,
        default=0,
        help="Allow expected_total and unique_id_count to differ by this amount (default: 0)",
    )
    p.add_argument(
        "--resource-types",
        default=None,
        help=(
            "Optional comma-separated list of resource types to check. "
            "If omitted, we infer from the NDJSON filenames."
        ),
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help=(
            "Initial HTTP timeout seconds for each API count URL. "
            "Each failed attempt doubles the timeout (default: 120)."
        ),
    )
    p.add_argument(
        "--csv-out",
        default=None,
        help=(
            "Write results to this CSV path. Default: <ndjson_dir>/verify_fhir_download_report.csv"
        ),
    )
    args = p.parse_args(argv)

    load_dotenv(override=True)
    creds = get_fhir_basic_auth()
    if creds is None:
        print("ERROR: missing FHIR_API_USERNAME/FHIR_API_PASSWORD in .env", file=sys.stderr)
        return 2

    ndjson_dir = Path(args.ndjson_dir)
    if not ndjson_dir.exists() or not ndjson_dir.is_dir():
        print(f"ERROR: ndjson_dir does not exist or is not a directory: {ndjson_dir}", file=sys.stderr)
        return 2

    # Determine what to check.
    ndjson_files = list(_iter_ndjson_files(ndjson_dir))
    if not ndjson_files:
        print(f"ERROR: no *.ndjson files found in {ndjson_dir}", file=sys.stderr)
        return 2

    if args.resource_types:
        resource_types = [x.strip() for x in args.resource_types.split(",") if x.strip()]
        if not resource_types:
            print("ERROR: --resource-types parsed to empty list", file=sys.stderr)
            return 2
        # Map resource types to expected filenames (best effort).
        file_map: dict[str, Path] = {}
        for f in ndjson_files:
            file_map[_resource_type_from_filename(f)] = f
        pairs: list[tuple[str, Path]] = []
        for rt in resource_types:
            if rt in file_map:
                pairs.append((rt, file_map[rt]))
            else:
                print(f"WARNING: no ndjson file found for resource type '{rt}' in {ndjson_dir}")
        if not pairs:
            print("ERROR: no matching NDJSON files for requested --resource-types", file=sys.stderr)
            return 2
    else:
        pairs = [(_resource_type_from_filename(f), f) for f in ndjson_files]

    csv_out = Path(args.csv_out) if args.csv_out else (ndjson_dir / "verify_fhir_download_report.csv")

    with httpx.Client(
        base_url=str(args.fhir_url).rstrip("/") + "/",
        auth=creds,
        headers={"Accept": "application/fhir+json"},
        # Per-request timeouts are set dynamically in fetch_expected_total.
        timeout=None,
        follow_redirects=True,
    ) as client:
        print(f"Starting verification against: {str(args.fhir_url).rstrip('/') + '/'}", file=sys.stderr)
        any_fail = write_csv_report(
            csv_out=csv_out,
            pairs=pairs,
            client=client,
            allow_delta=int(args.allow_delta),
            api_max_attempts_per_url=6,
            api_initial_timeout_seconds=float(args.timeout),
        )

    # Keep stdout minimal but helpful.
    print(f"Wrote CSV report: {csv_out}")

    if any_fail:
        print("Verification finished: FAIL", file=sys.stderr)
    else:
        print("Verification finished: PASS", file=sys.stderr)

    if any_fail:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
