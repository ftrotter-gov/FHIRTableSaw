#!/usr/bin/env python3
"""Repo-root runner for FHIRTableSaw.

This script is intended to be the "connective tissue" runner:

1. Load configuration from `.env`.
2. Download supported FHIR resources from the configured FHIR server into NDJSON files
   under `FHIR_API_CACHE_FOLDER` (always overwrite).
3. For each supported resource type, run the existing FAST pipeline:
   NDJSON -> DuckDB -> ViewDefinition -> CSV -> (optional) PostgreSQL.

Key design constraints:
* Use SQL-on-FHIR ViewDefinitions (`viewdefs/*.json`) for flattening.
* Do not embed credentials in source code.
* Prefer calling existing CLI scripts rather than re-implementing business logic.

Configuration (from `.env`):
* FHIR_SERVER_URL (optional; default: https://dev.cnpd.internal.cms.gov/fhir/)
* FHIR_API_USERNAME / FHIR_API_PASSWORD
* FHIR_API_CACHE_FOLDER (required)
* DATABASE_URL (optional; if reachable, we upload to Postgres)

Limiter/testing knobs are accepted as CLI args and passed through to the underlying
scripts.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# Ensure repo src/ is importable when running as `python go.py`.
REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fhir_tablesaw_3tier.env import load_dotenv  # noqa: E402


DEFAULT_FHIR_SERVER_URL = "https://dev.cnpd.internal.cms.gov/fhir/"


# Supported resource types are those the repo has ViewDefinitions for today.
# These match create_ndjson_from_api.py (and the viewdefs/ directory).
SUPPORTED_RESOURCE_TYPES: tuple[str, ...] = (
    "Practitioner",
    "PractitionerRole",
    "Organization",
    "OrganizationAffiliation",
    "Endpoint",
    "Location",
)


VIEWDEF_MAP: dict[str, str] = {
    "endpoint": "viewdefs/endpoint.json",
    "location": "viewdefs/location.json",
    "organization": "viewdefs/organization.json",
    "organizationaffiliation": "viewdefs/organization_affiliation.json",
    "practitioner": "viewdefs/practitioner.json",
    "practitionerrole": "viewdefs/practitioner_role.json",
}


def _snake_resource_type(resource_type: str) -> str:
    """Mirror create_ndjson_from_api._snake_file_name without importing a private function."""

    out: list[str] = []
    for i, ch in enumerate(resource_type):
        if ch.isupper() and i != 0:
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def _ndjson_path_for(*, cache_folder: Path, resource_type: str) -> Path:
    return cache_folder / f"{_snake_resource_type(resource_type)}.ndjson"


def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return v


def _postgres_reachable(*, database_url: str) -> bool:
    """Best-effort connectivity test.

    We keep this lightweight so go.py can decide whether to pass `--upload`.
    """

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        finally:
            engine.dispose()
    except Exception:
        return False


@dataclass(frozen=True)
class DownloadArgs:
    count: int
    stop_after_this_many: int | None
    progress_every: int
    retries: int
    backoff: float
    timeout: float
    log_dir: str | None
    resource_types: str | None
    print_urls: bool
    curl_on_error: bool
    curl_body_snippet_chars: int


@dataclass(frozen=True)
class ProcessArgs:
    batch_size: int
    limit: int | None
    temp_dir: str | None
    upload_mode: str
    no_upload: bool


def _build_download_cmd(*, fhir_server_url: str, cache_folder: Path, args: DownloadArgs) -> list[str]:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "create_ndjson_from_api.py"),
        fhir_server_url,
        str(cache_folder),
        "--count",
        str(args.count),
        "--progress-every",
        str(args.progress_every),
        "--retries",
        str(args.retries),
        "--backoff",
        str(args.backoff),
        "--timeout",
        str(args.timeout),
        "--curl-body-snippet-chars",
        str(args.curl_body_snippet_chars),
    ]

    if args.stop_after_this_many is not None:
        cmd += ["--stop-after-this-many", str(args.stop_after_this_many)]
    if args.log_dir is not None:
        cmd += ["--log-dir", str(args.log_dir)]
    if args.resource_types is not None:
        cmd += ["--resource-types", str(args.resource_types)]

    # BooleanOptionalAction args.
    cmd += ["--print-urls" if args.print_urls else "--no-print-urls"]
    cmd += ["--curl-on-error" if args.curl_on_error else "--no-curl-on-error"]

    return cmd


def _build_process_cmd(
    *,
    ndjson_path: Path,
    viewdef_path: str,
    duckdb_path: Path,
    csv_path: Path,
    args: ProcessArgs,
    do_upload: bool,
) -> list[str]:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "process_ndjson_fast.py"),
        str(ndjson_path),
        "--viewdef",
        str(REPO_ROOT / viewdef_path),
        "--duckdb-path",
        str(duckdb_path),
        "--csv-path",
        str(csv_path),
        "--batch-size",
        str(args.batch_size),
        "--upload-mode",
        str(args.upload_mode),
        "--force-reload",
        "--force-overwrite",
    ]
    if args.limit is not None:
        cmd += ["--limit", str(args.limit)]
    if args.temp_dir is not None:
        cmd += ["--temp-dir", str(args.temp_dir)]
    if do_upload and not args.no_upload:
        cmd += ["--upload"]
    return cmd


def _run_cmd(*, cmd: list[str], title: str) -> int:
    # Flush before running subprocess so banner prints appear before child output.
    print("\n" + "=" * 80, flush=True)
    print(title, flush=True)
    print("=" * 80, flush=True)
    print("Command:", flush=True)
    print("  " + " ".join(cmd), flush=True)
    print("=" * 80 + "\n", flush=True)

    sys.stdout.flush()
    cp = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True)
    return int(cp.returncode)


def _parse_args(argv: list[str] | None = None) -> tuple[DownloadArgs, ProcessArgs]:
    p = argparse.ArgumentParser(
        prog="go.py",
        description=(
            "Runner: download FHIR resources to NDJSON cache folder, then process via FAST pipeline. "
            "Configuration is loaded from .env; limiter/testing knobs are CLI args."
        ),
    )

    # Downloader pass-through (mirrors create_ndjson_from_api.py)
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--stop-after-this-many", dest="stop_after_this_many", type=int, default=None)
    p.add_argument("--progress-every", type=int, default=1000)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--backoff", type=float, default=0.5)
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--log-dir", default=None)
    p.add_argument(
        "--resource-types",
        default=None,
        help=(
            "Comma-separated list of FHIR resource types to download (defaults to supported set). "
            "Example: Practitioner,Organization"
        ),
    )
    p.add_argument(
        "--print-urls",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print paged search request URLs during download (default: true)",
    )
    p.add_argument(
        "--curl-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run curl diagnostics on HTTP/transport errors during download (default: true)",
    )
    p.add_argument("--curl-body-snippet-chars", type=int, default=2000)

    # FAST pipeline pass-through (mirrors scripts/process_ndjson_fast.py)
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--limit", type=int, default=None, help="Limit max resources processed (testing)")
    p.add_argument("--temp-dir", default=None, help="DuckDB temp directory")
    p.add_argument(
        "--upload-mode",
        choices=["replace", "append", "fail"],
        default="replace",
        help="PostgreSQL upload mode (default: replace)",
    )
    p.add_argument(
        "--no-upload",
        action="store_true",
        help="Do not upload to Postgres even if DATABASE_URL is reachable",
    )

    ns = p.parse_args(argv)

    download_args = DownloadArgs(
        count=int(ns.count),
        stop_after_this_many=ns.stop_after_this_many,
        progress_every=int(ns.progress_every),
        retries=int(ns.retries),
        backoff=float(ns.backoff),
        timeout=float(ns.timeout),
        log_dir=ns.log_dir,
        resource_types=ns.resource_types,
        print_urls=bool(ns.print_urls),
        curl_on_error=bool(ns.curl_on_error),
        curl_body_snippet_chars=int(ns.curl_body_snippet_chars),
    )

    process_args = ProcessArgs(
        batch_size=int(ns.batch_size),
        limit=ns.limit,
        temp_dir=ns.temp_dir,
        upload_mode=str(ns.upload_mode),
        no_upload=bool(ns.no_upload),
    )
    return download_args, process_args


def main(argv: list[str] | None = None) -> int:
    # Load .env as the source of truth.
    load_dotenv(override=True)

    download_args, process_args = _parse_args(argv)

    # Core config from .env
    cache_folder = Path(_require_env("FHIR_API_CACHE_FOLDER"))
    cache_folder.mkdir(parents=True, exist_ok=True)

    # Ensure credentials exist (create_ndjson_from_api.py will also validate).
    _require_env("FHIR_API_USERNAME")
    _require_env("FHIR_API_PASSWORD")

    fhir_server_url = os.environ.get("FHIR_SERVER_URL") or DEFAULT_FHIR_SERVER_URL

    # Decide whether to upload to Postgres.
    db_url = os.environ.get("DATABASE_URL")
    do_upload = False
    if db_url and not process_args.no_upload:
        do_upload = _postgres_reachable(database_url=db_url)

    print("=" * 80)
    print("FHIRTableSaw go.py runner")
    print("=" * 80)
    print(f"FHIR server URL: {fhir_server_url}")
    print(f"Cache folder:   {cache_folder}")
    print(f"Postgres upload: {'YES' if do_upload and not process_args.no_upload else 'NO'}")
    print("=" * 80, flush=True)

    # --- Stage 1: Download resources to NDJSON (overwrite). ---
    dl_cmd = _build_download_cmd(
        fhir_server_url=fhir_server_url,
        cache_folder=cache_folder,
        args=download_args,
    )
    rc = _run_cmd(cmd=dl_cmd, title="STAGE 1: Download FHIR -> NDJSON (overwrite cache)")
    if rc != 0:
        print(f"ERROR: download stage failed with exit code {rc}")
        return rc

    # Determine which resource types were requested for download.
    requested_types = list(SUPPORTED_RESOURCE_TYPES)
    if download_args.resource_types:
        requested_types = [x.strip() for x in download_args.resource_types.split(",") if x.strip()]

    # --- Stage 2+: Process each resource type via FAST pipeline. ---
    failures: list[str] = []
    for rt in requested_types:
        ndjson_path = _ndjson_path_for(cache_folder=cache_folder, resource_type=rt)
        if not ndjson_path.exists():
            failures.append(f"{rt}: expected NDJSON not found at {ndjson_path}")
            continue

        view_key = rt.lower()
        viewdef = VIEWDEF_MAP.get(view_key)
        if not viewdef:
            failures.append(f"{rt}: no ViewDefinition mapping for key '{view_key}'")
            continue

        # Compute deterministic output paths in the cache folder.
        viewdef_file = REPO_ROOT / viewdef
        try:
            viewdef_name = json.loads(viewdef_file.read_text(encoding="utf-8")).get(
                "name", view_key
            )
        except Exception as ex:  # noqa: BLE001
            failures.append(f"{rt}: failed reading ViewDefinition {viewdef_file}: {ex}")
            continue

        duckdb_path = ndjson_path.with_suffix(".duckdb")
        csv_path = ndjson_path.parent / f"{ndjson_path.stem}_{viewdef_name}.csv"

        # Ensure overwrite semantics for all artifacts.
        try:
            if duckdb_path.exists():
                duckdb_path.unlink()
            if csv_path.exists():
                csv_path.unlink()
        except Exception as ex:  # noqa: BLE001
            failures.append(f"{rt}: failed removing old artifacts: {ex}")
            continue

        cmd = _build_process_cmd(
            ndjson_path=ndjson_path,
            viewdef_path=viewdef,
            duckdb_path=duckdb_path,
            csv_path=csv_path,
            args=process_args,
            do_upload=do_upload,
        )
        rc2 = _run_cmd(cmd=cmd, title=f"PROCESS: {rt}")
        if rc2 != 0:
            failures.append(f"{rt}: processing failed with exit code {rc2}")

    print("\n" + "=" * 80)
    print("RUN COMPLETE")
    print("=" * 80)
    if failures:
        print(f"Failures: {len(failures)}")
        for f in failures:
            print(f"- {f}")
        return 1

    print("✓ All requested resource types processed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
