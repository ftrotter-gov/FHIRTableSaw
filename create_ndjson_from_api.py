"""Export NDJSON from a Basic-Auth protected FHIR API.

This is a lightweight utility intended to mirror exactly what a FHIR server returns,
writing one *raw* FHIR Resource JSON per line (NDJSON) for a small set of resource
types.

It:
* Accepts a FHIR base URL and an output directory.
* Loads Basic Auth credentials from `.env`.
* Fetches resources via FHIR search paging (Bundle.link[relation=next]).
* Writes NDJSON files, overwriting existing files.
* Retries transient HTTP failures with exponential backoff.
* Logs failures without aborting the whole run.

Environment variables (loaded from `.env`):

Preferred:
* FHIR_API_USERNAME
* FHIR_API_PASSWORD

Accepted aliases:
* FHIR_USERNAME
* FHIR_PASSWORD
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import httpx


def _ensure_src_on_path() -> None:
    """Allow running this repo-root script without installation.

    When running `python create_ndjson_from_api.py ...`, Python may not have `src/`
    on sys.path. We add it so we can reuse the project's minimal `.env` loader.
    """

    repo_root = Path(__file__).resolve().parent
    src = repo_root / "src"
    if src.exists() and str(src) not in sys.path:
        sys.path.insert(0, str(src))


_ensure_src_on_path()

from fhir_tablesaw_3tier.env import load_dotenv  # noqa: E402


RESOURCE_TYPES: tuple[str, ...] = (
    "Practitioner",
    "PractitionerRole",
    "Organization",
    "OrganizationAffiliation",
    "Endpoint",
    "Location",
)


def _snake_file_name(resource_type: str) -> str:
    # Keep it simple and predictable for downstream scripts.
    out: list[str] = []
    for i, ch in enumerate(resource_type):
        if ch.isupper() and i != 0:
            out.append("_")
        out.append(ch.lower())
    return "".join(out) + ".ndjson"


def _safe_path_component(value: str) -> str:
    """Return a filesystem-safe component."""

    if not value:
        return "unknown"
    out = []
    for ch in value:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    s = "".join(out).strip("._")
    return s or "unknown"


def _now_iso() -> str:
    return time.strftime("%Y%m%dT%H%M%S", time.localtime())


def _get_basic_auth_from_env() -> tuple[str, str]:
    """Return (username, password) using preferred env var names."""

    username = os.environ.get("FHIR_API_USERNAME") or os.environ.get("FHIR_USERNAME")
    password = os.environ.get("FHIR_API_PASSWORD") or os.environ.get("FHIR_PASSWORD")
    if not username or not password:
        raise ValueError(
            "Missing Basic Auth credentials. Set FHIR_API_USERNAME/FHIR_API_PASSWORD (preferred) "
            "or FHIR_USERNAME/FHIR_PASSWORD in .env"
        )
    return username, password


def _iter_bundle_entries(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for entry in bundle.get("entry", []) or []:
        res = entry.get("resource")
        if isinstance(res, dict):
            yield res


def _find_next_link(bundle: dict[str, Any]) -> str | None:
    for link in bundle.get("link", []) or []:
        if link.get("relation") == "next" and link.get("url"):
            return str(link.get("url"))
    return None


def _extract_bundle(
    payload: dict[str, Any],
    *,
    max_depth: int = 3,
) -> tuple[dict[str, Any], str | None, str]:
    """Detect and unwrap a Bundle nested in a wrapper JSON.

    Why:
      Most FHIR servers return a top-level Bundle for search, but some wrap it
      inside an extra JSON envelope.

    Supported patterns (recursively, up to max_depth):
      1) Standard:
         {"resourceType":"Bundle", ...}
      2) Wrapper with common paging keys:
         {
           "next": "http(s)://...",
           "previous": ...,
           "results": {"resourceType":"Bundle", ...}
         }
      3) Generic wrapper with any nested dict containing a Bundle
         (we take the first matching dict encountered in a stable key order).

    Returns:
      (bundle_dict, explicit_next_url, unwrap_path)

    where:
      - explicit_next_url overrides Bundle.link[next] if present.
      - unwrap_path is a dotted key path (e.g. "$" or "$.results").
    """

    def _walk(obj: Any, *, depth: int, path: str) -> tuple[dict[str, Any], str | None, str] | None:
        if not isinstance(obj, dict):
            return None

        if obj.get("resourceType") == "Bundle":
            return obj, None, path

        if depth >= max_depth:
            return None

        # If it looks like a paging wrapper, prefer these keys.
        next_url = obj.get("next")
        for key in ("results", "result", "bundle", "data"):
            child = obj.get(key)
            out = _walk(child, depth=depth + 1, path=f"{path}.{key}")
            if out is not None:
                b, _, p = out
                return b, str(next_url) if next_url else None, p

        # Otherwise, scan nested dicts.
        for k in sorted(obj.keys()):
            child = obj.get(k)
            if isinstance(child, dict):
                out = _walk(child, depth=depth + 1, path=f"{path}.{k}")
                if out is not None:
                    b, explicit_next, p = out
                    # Bubble up a wrapper-level next if present.
                    if next_url and not explicit_next:
                        explicit_next = str(next_url)
                    return b, explicit_next, p

        return None

    found = _walk(payload, depth=0, path="$")
    if found is None:
        return payload, None, "$"
    return found


@dataclass(frozen=True)
class RetryConfig:
    retries: int = 5
    backoff_seconds: float = 0.5
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class UrlPrintConfig:
    print_urls: bool = True


@dataclass(frozen=True)
class CurlDebugConfig:
    """Controls curl-based diagnostics.

    We only invoke curl when we hit an error (HTTP status errors or transport
    errors) so normal runs don't incur extra overhead.
    """

    curl_on_error: bool = True
    body_snippet_chars: int = 2000


def _sleep_backoff(backoff_seconds: float, attempt: int) -> None:
    # Exponential backoff: b, 2b, 4b, ... capped at 60s
    delay = min(backoff_seconds * (2 ** max(attempt - 1, 0)), 60.0)
    time.sleep(delay)


def _parse_retry_after_seconds(headers: httpx.Headers) -> float | None:
    ra = headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except ValueError:
        # Ignore date-form Retry-After for now.
        return None


def _build_full_url(client: httpx.Client, url: str, params: dict[str, str] | None) -> httpx.URL:
    if url.startswith("http://") or url.startswith("https://"):
        full = httpx.URL(url)
    else:
        full = client.base_url.join(url)
    if params:
        full = full.copy_merge_params(params)
    return full


def _curl_diagnose(
    *,
    full_url: httpx.URL,
    username: str,
    password: str,
    timeout_seconds: float,
    accept_header: str,
    body_snippet_chars: int,
) -> dict[str, Any]:
    """Run curl and capture headers/body snippets for debugging.

    IMPORTANT: This function must never print credentials.
    """

    # NOTE: This still passes credentials to curl. We do not log them. This is
    # meant for local debugging only.
    cmd = [
        "curl",
        "-sS",
        "-L",
        "--max-time",
        str(timeout_seconds),
        "-u",
        f"{username}:{password}",
        "-H",
        f"Accept: {accept_header}",
        "-D",
        "-",  # headers to stdout
        "-o",
        "-",  # body to stdout
        str(full_url),
    ]

    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return {
            "curl_available": False,
            "curl_error": "curl binary not found on PATH",
        }

    out = cp.stdout or ""
    err = cp.stderr or ""

    # Curl writes headers then a blank line then body. With -L, it can include
    # multiple header blocks; we keep the full header text but try to parse the
    # final HTTP status line.
    header_text = ""
    body_text = ""
    if "\r\n\r\n" in out:
        header_text, body_text = out.split("\r\n\r\n", 1)
    elif "\n\n" in out:
        header_text, body_text = out.split("\n\n", 1)
    else:
        # Unexpected format.
        body_text = out

    status_line = None
    status_code = None
    for line in header_text.splitlines():
        if line.startswith("HTTP/"):
            status_line = line.strip()
    if status_line:
        parts = status_line.split()
        if len(parts) >= 2:
            try:
                status_code = int(parts[1])
            except ValueError:
                status_code = None

    # Avoid logging huge responses.
    body_snippet = body_text[: max(int(body_snippet_chars), 0)]

    # Remove lines that could be noisy; keep useful auth hints.
    filtered_headers: list[str] = []
    for line in header_text.splitlines():
        if line.lower().startswith("set-cookie:"):
            continue
        filtered_headers.append(line)

    return {
        "curl_available": True,
        "curl_returncode": cp.returncode,
        "curl_stderr_snippet": err[:2000],
        "curl_status_line": status_line,
        "curl_status_code": status_code,
        "curl_headers": "\n".join(filtered_headers)[:8000],
        "curl_body_snippet": body_snippet,
    }


def _friendly_http_diagnosis(status_code: int | None, *, www_authenticate: str | None) -> str | None:
    if status_code is None:
        return None
    if status_code == 401:
        msg = "HTTP 401 Unauthorized: likely bad username/password or server doesn't accept Basic Auth"
        if www_authenticate:
            msg += f" (WWW-Authenticate: {www_authenticate})"
        return msg
    if status_code == 403:
        return "HTTP 403 Forbidden: credentials are valid but not authorized for this endpoint/resource"
    if status_code == 404:
        return "HTTP 404 Not Found: check the base URL (missing /fhir?) or resource type path"
    if status_code == 406:
        return "HTTP 406 Not Acceptable: server may not like Accept: application/fhir+json"
    if status_code == 415:
        return "HTTP 415 Unsupported Media Type: server may require different headers"
    if status_code == 429:
        return "HTTP 429 Too Many Requests: rate limited (try reducing _count or adding delays)"
    if 500 <= status_code <= 599:
        return f"HTTP {status_code} Server Error: server-side problem (may be transient)"
    return f"HTTP {status_code} error"


def _print_curl_debug(curl_result: dict[str, Any]) -> None:
    """Print a concise curl debug summary to stdout."""

    if not curl_result.get("curl_available", True):
        print(f"curl debug unavailable: {curl_result.get('curl_error')}")
        return

    rc = curl_result.get("curl_returncode")
    if rc is not None:
        print(f"curl return code: {rc}")

    stderr = curl_result.get("curl_stderr_snippet")
    if stderr:
        print("curl stderr snippet:")
        print(stderr)

    status_line = curl_result.get("curl_status_line")
    if status_line:
        print(f"curl status: {status_line}")
    headers = curl_result.get("curl_headers")
    if headers:
        print("curl headers (filtered):")
        print(headers)
    body = curl_result.get("curl_body_snippet")
    if body:
        print("curl body snippet:")
        print(body)


def _request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    params: dict[str, str] | None,
    retry: RetryConfig,
    log_fn,
    context: dict[str, Any],
    curl_debug: CurlDebugConfig,
    curl_auth: tuple[str, str] | None,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max(retry.retries, 1) + 1):
        try:
            r = client.request(method, url, params=params)
            # Raise on error statuses so we handle retries consistently.
            r.raise_for_status()
            return r
        except httpx.HTTPStatusError as ex:
            last_exc = ex
            status = ex.response.status_code

            www_auth = ex.response.headers.get("WWW-Authenticate")
            friendly = _friendly_http_diagnosis(status, www_authenticate=www_auth)
            if friendly:
                print(f"ERROR: {friendly}")
            else:
                print(f"ERROR: HTTP {status}")

            # Retry on 429 and 5xx.
            if status == 429 or 500 <= status <= 599:
                retry_after = _parse_retry_after_seconds(ex.response.headers)
                event: dict[str, Any] = {
                    **context,
                    "kind": "http_status_error",
                    "attempt": attempt,
                    "status_code": status,
                    "retry_after": retry_after,
                    "www_authenticate": www_auth,
                    "friendly": friendly,
                    "error": str(ex),
                }

                if curl_debug.curl_on_error and curl_auth is not None:
                    full_url = _build_full_url(client, url, params)
                    event["curl"] = _curl_diagnose(
                        full_url=full_url,
                        username=curl_auth[0],
                        password=curl_auth[1],
                        timeout_seconds=retry.timeout_seconds,
                        accept_header=client.headers.get("Accept", "application/fhir+json"),
                        body_snippet_chars=curl_debug.body_snippet_chars,
                    )

                    # Print curl diagnostics to stdout on first and last attempt.
                    if attempt == 1 or attempt >= retry.retries:
                        print("--- curl diagnostics (on error) ---")
                        _print_curl_debug(event["curl"])
                        print("--- end curl diagnostics ---")

                log_fn(event)
                if attempt >= retry.retries:
                    raise
                if retry_after is not None:
                    time.sleep(min(retry_after, 120.0))
                else:
                    _sleep_backoff(retry.backoff_seconds, attempt)
                continue

            # Non-retryable 4xx -> log and re-raise.
            event2: dict[str, Any] = {
                **context,
                "kind": "http_status_error",
                "attempt": attempt,
                "status_code": status,
                "www_authenticate": www_auth,
                "friendly": friendly,
                "error": str(ex),
            }
            if curl_debug.curl_on_error and curl_auth is not None:
                full_url = _build_full_url(client, url, params)
                event2["curl"] = _curl_diagnose(
                    full_url=full_url,
                    username=curl_auth[0],
                    password=curl_auth[1],
                    timeout_seconds=retry.timeout_seconds,
                    accept_header=client.headers.get("Accept", "application/fhir+json"),
                    body_snippet_chars=curl_debug.body_snippet_chars,
                )

                # Non-retryable: always print curl diagnostics.
                print("--- curl diagnostics (on error) ---")
                _print_curl_debug(event2["curl"])
                print("--- end curl diagnostics ---")

            log_fn(event2)
            raise
        except (httpx.TimeoutException, httpx.NetworkError) as ex:
            last_exc = ex
            print(
                "ERROR: transport error (DNS/TLS/connectivity/base URL issue): "
                f"{type(ex).__name__}: {ex}"
            )
            event3: dict[str, Any] = {
                **context,
                "kind": "transport_error",
                "attempt": attempt,
                "error": str(ex),
            }
            if curl_debug.curl_on_error and curl_auth is not None:
                full_url = _build_full_url(client, url, params)
                event3["curl"] = _curl_diagnose(
                    full_url=full_url,
                    username=curl_auth[0],
                    password=curl_auth[1],
                    timeout_seconds=retry.timeout_seconds,
                    accept_header=client.headers.get("Accept", "application/fhir+json"),
                    body_snippet_chars=curl_debug.body_snippet_chars,
                )

                # Print curl diagnostics to stdout on first and last attempt.
                if attempt == 1 or attempt >= retry.retries:
                    print("--- curl diagnostics (on error) ---")
                    _print_curl_debug(event3["curl"])
                    print("--- end curl diagnostics ---")
            log_fn(event3)
            if attempt >= retry.retries:
                raise
            _sleep_backoff(retry.backoff_seconds, attempt)
            continue
        except Exception as ex:  # noqa: BLE001
            # Unexpected error: don't spin forever.
            last_exc = ex
            log_fn({**context, "kind": "unexpected_error", "attempt": attempt, "error": str(ex)})
            raise

    # Should be unreachable.
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("request retry loop failed unexpectedly")


def _make_failure_logger(*, log_dir: Path, resource_type: str):
    safe_type = _safe_path_component(resource_type)
    out_dir = log_dir / safe_type
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / "failures.jsonl"

    def _log(event: dict[str, Any]) -> None:
        event = {"ts": _now_iso(), **event}
        with jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    return _log


def fetch_resources(
    *,
    client: httpx.Client,
    resource_type: str,
    count: int,
    hard_limit: int | None,
    retry: RetryConfig,
    log_dir: Path,
    url_print: UrlPrintConfig,
    curl_debug: CurlDebugConfig,
    curl_auth: tuple[str, str] | None,
) -> Iterable[dict[str, Any]]:
    """Yield raw FHIR resources of a given type using paging."""

    log_fn = _make_failure_logger(log_dir=log_dir, resource_type=resource_type)

    # Some servers use non-FHIR `page_size` rather than `_count`.
    # We send both on the first request; for next links we follow server-provided URLs.
    # However, some servers reject requests with both parameters, so we'll retry with just _count if needed.
    params: dict[str, str] | None = {
        "_count": str(count),
        "page_size": str(count),
        "_total": "none",
    }
    url: str = resource_type
    seen = 0
    page_num = 0
    first_request = True

    while True:
        ctx = {"resource_type": resource_type, "url": url, "params": params}

        if url_print.print_urls:
            full = _build_full_url(client, url, params)
            print(f"GET {full}")

        # On the first request, if we get a 400 error with both _count and page_size,
        # retry with just _count (some servers reject both parameters together)
        try:
            r = _request_with_retry(
                client,
                "GET",
                url,
                params=params,
                retry=retry,
                log_fn=log_fn,
                context=ctx,
                curl_debug=curl_debug,
                curl_auth=curl_auth,
            )
        except httpx.HTTPStatusError as ex:
            # If this is the first request and we got a 400-level error with both params,
            # retry with just _count
            if first_request and params is not None and "page_size" in params and 400 <= ex.response.status_code < 500:
                print(f"WARNING: Server rejected request with both _count and page_size (HTTP {ex.response.status_code})")
                print("Retrying with only _count parameter...")
                log_fn({
                    **ctx,
                    "kind": "parameter_conflict_retry",
                    "status_code": ex.response.status_code,
                    "original_params": params,
                })
                
                # Retry with just _count
                params = {"_count": str(count), "_total": "none"}
                ctx = {"resource_type": resource_type, "url": url, "params": params}
                
                if url_print.print_urls:
                    full = _build_full_url(client, url, params)
                    print(f"GET {full}")
                
                r = _request_with_retry(
                    client,
                    "GET",
                    url,
                    params=params,
                    retry=retry,
                    log_fn=log_fn,
                    context=ctx,
                    curl_debug=curl_debug,
                    curl_auth=curl_auth,
                )
            else:
                # Not a parameter conflict or not the first request - re-raise
                raise
        
        first_request = False

        # Diagnostics for unexpected payloads.
        status_code = r.status_code
        content_type = r.headers.get("Content-Type")

        # Some servers return non-JSON on errors; above raise_for_status should
        # catch most of those. Here we still guard JSON decode.
        try:
            payload = r.json()
        except Exception as ex:  # noqa: BLE001
            log_fn(
                {
                    **ctx,
                    "kind": "json_decode_error",
                    "error": str(ex),
                    "response_text_snippet": (r.text or "")[:2000],
                    "status_code": status_code,
                    "content_type": content_type,
                }
            )

            print(
                "ERROR: response was not valid JSON. "
                f"status={status_code} content-type={content_type}"
            )
            snippet = (r.text or "")[:2000]
            if snippet:
                print("response body snippet:")
                print(snippet)

            if curl_debug.curl_on_error and curl_auth is not None:
                full_url = _build_full_url(client, url, params)
                curl_res = _curl_diagnose(
                    full_url=full_url,
                    username=curl_auth[0],
                    password=curl_auth[1],
                    timeout_seconds=retry.timeout_seconds,
                    accept_header=client.headers.get("Accept", "application/fhir+json"),
                    body_snippet_chars=curl_debug.body_snippet_chars,
                )
                print("--- curl diagnostics (unexpected payload) ---")
                _print_curl_debug(curl_res)
                print("--- end curl diagnostics ---")
                log_fn({**ctx, "kind": "curl_unexpected_payload", "curl": curl_res})
            raise

        # Some servers provide helpful totals in the wrapper.
        total_hint = None
        if isinstance(payload, dict) and isinstance(payload.get("count"), int):
            total_hint = int(payload["count"])

        bundle, wrapped_next, unwrap_path = _extract_bundle(payload)

        # unwrap_path is available for debugging if needed, but we keep stdout clean.

        if bundle.get("resourceType") != "Bundle":
            print(
                "ERROR: expected FHIR Bundle but got unexpected JSON payload. "
                f"status={status_code} content-type={content_type} "
                f"resourceType={bundle.get('resourceType')}"
            )
            try:
                print("response json snippet:")
                print(json.dumps(bundle, ensure_ascii=False)[:2000])
            except Exception:  # noqa: BLE001
                pass

            if curl_debug.curl_on_error and curl_auth is not None:
                full_url = _build_full_url(client, url, params)
                curl_res = _curl_diagnose(
                    full_url=full_url,
                    username=curl_auth[0],
                    password=curl_auth[1],
                    timeout_seconds=retry.timeout_seconds,
                    accept_header=client.headers.get("Accept", "application/fhir+json"),
                    body_snippet_chars=curl_debug.body_snippet_chars,
                )
                print("--- curl diagnostics (unexpected payload) ---")
                _print_curl_debug(curl_res)
                print("--- end curl diagnostics ---")
                log_fn({**ctx, "kind": "curl_unexpected_payload", "curl": curl_res})

            log_fn(
                {
                    **ctx,
                    "kind": "unexpected_resourceType",
                    "bundle_resourceType": bundle.get("resourceType"),
                    "status_code": status_code,
                    "content_type": content_type,
                }
            )
            raise ValueError(
                f"Expected Bundle for {resource_type} search, got {bundle.get('resourceType')}"
            )

        page_num += 1

        # If we didn't get a wrapper total, fall back to Bundle.total when present.
        if total_hint is None and isinstance(bundle.get("total"), int):
            total_hint = int(bundle["total"])

        entries = list(_iter_bundle_entries(bundle))
        page_entries = len(entries)
        projected_seen = seen + page_entries
        if total_hint is not None:
            print(
                f"{resource_type}: page={page_num} page_entries={page_entries} "
                f"downloaded_this_type={projected_seen} of ~{total_hint}"
            )
        else:
            print(
                f"{resource_type}: page={page_num} page_entries={page_entries} "
                f"downloaded_this_type={projected_seen}"
            )

        for res in entries:
            yield res
            seen += 1
            if hard_limit is not None and seen >= hard_limit:
                return

        # Prefer wrapper next URL if present.
        next_url = wrapped_next or _find_next_link(bundle)
        if not next_url:
            return

        # After the first request, the server typically provides an absolute
        # URL. httpx can request absolute URLs even with base_url configured.
        url = next_url
        params = None


def export_ndjson(
    *,
    fhir_base_url: str,
    output_dir: Path,
    resource_types: Iterable[str],
    count: int,
    hard_limit: int | None,
    retry: RetryConfig,
    log_dir: Path,
    url_print: UrlPrintConfig,
    curl_debug: CurlDebugConfig,
    progress_every: int,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    username, password = _get_basic_auth_from_env()

    totals: dict[str, int] = {}
    failures: dict[str, int] = {}

    overall_downloaded = 0
    overall_written = 0
    overall_started = time.time()

    with httpx.Client(
        base_url=fhir_base_url.rstrip("/") + "/",
        timeout=retry.timeout_seconds,
        auth=(username, password),
        headers={"Accept": "application/fhir+json"},
        # HTTP/2 can help but sometimes breaks behind proxies; keep default off here.
        http2=False,
        follow_redirects=True,
    ) as client:
        for rt in resource_types:
            out_path = output_dir / _snake_file_name(rt)
            # Overwrite.
            out_path.write_text("", encoding="utf-8")

            log_fn = _make_failure_logger(log_dir=log_dir, resource_type=rt)
            downloaded = 0
            written = 0
            failed = 0
            started = time.time()

            print(f"--- Exporting {rt} -> {out_path} ---")
            try:
                with out_path.open("a", encoding="utf-8") as f:
                    for res in fetch_resources(
                        client=client,
                        resource_type=rt,
                        count=count,
                        hard_limit=hard_limit,
                        retry=retry,
                        log_dir=log_dir,
                        url_print=url_print,
                        curl_debug=curl_debug,
                        curl_auth=(username, password),
                    ):
                        downloaded += 1
                        overall_downloaded += 1

                        try:
                            f.write(json.dumps(res, ensure_ascii=False) + "\n")
                            written += 1
                            overall_written += 1

                            if progress_every and downloaded % max(progress_every, 1) == 0:
                                elapsed = max(time.time() - started, 0.000001)
                                rate = downloaded / elapsed
                                overall_elapsed = max(time.time() - overall_started, 0.000001)
                                overall_rate = overall_downloaded / overall_elapsed
                                print(
                                    f"{rt}: downloaded={downloaded} written={written} failed_write={failed} "
                                    f"elapsed={elapsed:0.1f}s rate={rate:0.1f}/s | "
                                    f"overall_downloaded={overall_downloaded} overall_written={overall_written} "
                                    f"overall_rate={overall_rate:0.1f}/s"
                                )
                        except Exception as ex:  # noqa: BLE001
                            failed += 1
                            log_fn(
                                {
                                    "resource_type": rt,
                                    "kind": "write_error",
                                    "error": str(ex),
                                    "traceback": "".join(
                                        traceback.format_exception(type(ex), ex, ex.__traceback__)
                                    ),
                                    "resource_id": res.get("id"),
                                }
                            )
                            # Continue exporting.
                            continue
            except Exception as ex:  # noqa: BLE001
                failed += 1
                print(f"ERROR: export of {rt} failed: {type(ex).__name__}: {ex}")
                log_fn(
                    {
                        "resource_type": rt,
                        "kind": "export_failed",
                        "error": str(ex),
                        "traceback": "".join(traceback.format_exception(type(ex), ex, ex.__traceback__)),
                    }
                )
                # Keep going to other resource types.

            totals[rt] = written
            failures[rt] = failed
            elapsed = max(time.time() - started, 0.000001)
            print(
                f"{rt}: DONE downloaded={downloaded} written={written} failed_write={failed} "
                f"elapsed={elapsed:0.1f}s"
            )

    print("\n--- Export summary ---")
    for rt in resource_types:
        print(f"{rt}: written={totals.get(rt, 0)} failed={failures.get(rt, 0)}")
    overall_elapsed = max(time.time() - overall_started, 0.000001)
    print(
        f"OVERALL: downloaded={overall_downloaded} written={overall_written} "
        f"elapsed={overall_elapsed:0.1f}s rate={overall_downloaded / overall_elapsed:0.1f}/s"
    )
    print(f"Output dir: {output_dir}")
    print(f"Log dir: {log_dir}")
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="create_ndjson_from_api.py",
        description="Export raw FHIR resources to NDJSON using Basic Auth credentials from .env",
    )
    p.add_argument(
        "fhir_base_url",
        help="FHIR base URL (e.g. https://example.org/fhir)",
    )
    p.add_argument(
        "output_dir",
        help="Directory to write NDJSON files",
    )
    p.add_argument(
        "--count",
        type=int,
        default=1000,
        help="FHIR paging size (_count). Default: 1000",
    )
    p.add_argument(
        "--stop-after-this-many",
        dest="stop_after_this_many",
        type=int,
        default=None,
        help="Stop after exporting N resources of each type (debugging)",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Print progress every N downloaded resources per type (default: 1000)",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Retry attempts for transient HTTP failures. Default: 5",
    )
    p.add_argument(
        "--backoff",
        type=float,
        default=0.5,
        help="Base backoff seconds for retries (exponential). Default: 0.5",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout seconds. Default: 30",
    )
    p.add_argument(
        "--log-dir",
        default=None,
        help="Directory to write failure logs (default: ./log/create_ndjson_from_api)",
    )
    p.add_argument(
        "--resource-types",
        default=None,
        help="Comma-separated list of resource types to export (default: the NDH slice)",
    )
    p.add_argument(
        "--print-urls",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print the exact request URL for each paged search request (default: true)",
    )
    p.add_argument(
        "--curl-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "On HTTP/transport errors, run curl to capture response headers/body snippets for debugging "
            "(default: true)"
        ),
    )
    p.add_argument(
        "--curl-body-snippet-chars",
        type=int,
        default=2000,
        help="Max response body chars to include in curl debug logs (default: 2000)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_dotenv(override=True)

    resource_types = list(RESOURCE_TYPES)
    if args.resource_types:
        resource_types = [x.strip() for x in args.resource_types.split(",") if x.strip()]
        if not resource_types:
            raise ValueError("--resource-types was provided but parsed to an empty list")

    out_dir = Path(args.output_dir)
    log_dir = Path(args.log_dir) if args.log_dir else Path("log") / "create_ndjson_from_api"

    retry = RetryConfig(
        retries=int(args.retries),
        backoff_seconds=float(args.backoff),
        timeout_seconds=float(args.timeout),
    )

    url_print = UrlPrintConfig(print_urls=bool(args.print_urls))

    curl_debug = CurlDebugConfig(
        curl_on_error=bool(args.curl_on_error),
        body_snippet_chars=int(args.curl_body_snippet_chars),
    )

    return export_ndjson(
        fhir_base_url=str(args.fhir_base_url),
        output_dir=out_dir,
        resource_types=resource_types,
        count=int(args.count),
        hard_limit=args.stop_after_this_many,
        retry=retry,
        log_dir=log_dir,
        url_print=url_print,
        curl_debug=curl_debug,
        progress_every=int(args.progress_every),
    )


if __name__ == "__main__":
    raise SystemExit(main())
