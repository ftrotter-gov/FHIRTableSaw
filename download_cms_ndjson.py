#!/usr/bin/env python3
"""
Simple script to download FHIR resources from CMS API to NDJSON files.

This script handles the CMS FHIR API authentication and downloads resources
to a specified directory. It's meant to be a simple, focused downloader.

Usage:
    python download_cms_ndjson.py output_directory

Example:
    python download_cms_ndjson.py /Users/ftrotter/internal_npd_fhir_api_ndjson
"""

import argparse
import os
import subprocess
import sys
import signal
import shutil
import threading
import time
import re
from pathlib import Path


# Ensure repo src/ is importable
REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fhir_tablesaw_3tier.env import load_dotenv


# CMS internal FHIR server - this is the actual CMS server, not the test server
CMS_FHIR_URL = "https://dev.cnpd.internal.cms.gov/fhir/"


DEFAULT_RESOURCE_TYPES: tuple[str, ...] = (
    "Practitioner",
    "PractitionerRole",
    "Organization",
    "OrganizationAffiliation",
    "Endpoint",
    "Location",
)


_RUNNING_PROCS_LOCK = threading.Lock()
_RUNNING_PROCS: dict[str, subprocess.Popen] = {}


def _require_env(*, name):
    """Get required environment variable or exit."""
    value = os.environ.get(name)
    if not value or value.strip() == "":
        print(f"❌ ERROR: Required environment variable '{name}' is not set in .env")
        sys.exit(1)
    return value


def _parse_resource_types(arg: str | None) -> list[str]:
    if arg is None:
        return list(DEFAULT_RESOURCE_TYPES)
    rts = [x.strip() for x in arg.split(",") if x.strip()]
    if not rts:
        raise ValueError("--resource-types was provided but parsed to an empty list")
    return rts


def _snake_file_name(resource_type: str) -> str:
    """Match create_ndjson_from_api.py's filename convention (snake_case.ndjson)."""

    out: list[str] = []
    for i, ch in enumerate(resource_type):
        if ch.isupper() and i != 0:
            out.append("_")
        out.append(ch.lower())
    return "".join(out) + ".ndjson"


def _build_create_ndjson_cmd(
    *,
    fhir_url: str,
    output_dir: Path,
    count: int,
    stop_after_this_many: int | None,
    resource_type: str,
) -> list[str]:
    cmd = [
        sys.executable,
        "-u",  # unbuffered so progress prints immediately even when piped
        str(REPO_ROOT / "create_ndjson_from_api.py"),
        fhir_url,
        str(output_dir),
        "--count",
        str(count),
        "--resource-types",
        resource_type,
        # Generous timeouts/retries to reduce timeout errors.
        "--retries",
        "6",
        "--timeout",
        "120",
    ]
    if stop_after_this_many:
        cmd.extend(["--stop-after-this-many", str(stop_after_this_many)])
    return cmd


def _resource_state_dir(*, output_dir: Path, resource_type: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in resource_type).strip("._")
    safe = safe or "unknown"
    return output_dir / "download_state" / safe


def _reset_resource_download_state(*, output_dir: Path, resource_type: str) -> None:
    ndjson_path = output_dir / _snake_file_name(resource_type)
    state_dir = _resource_state_dir(output_dir=output_dir, resource_type=resource_type)

    if ndjson_path.exists():
        ndjson_path.unlink()
    if state_dir.exists():
        shutil.rmtree(state_dir)


def _build_verify_cmd(*, ndjson_dir: Path, fhir_url: str, allow_delta: int, resource_type: str) -> list[str]:
    # Verify only a single resource type.
    return [
        sys.executable,
        "-u",
        str(REPO_ROOT / "verify_fhir_download.py"),
        str(ndjson_dir),
        str(fhir_url),
        "--allow-delta",
        str(int(allow_delta)),
        "--resource-types",
        resource_type,
        "--timeout",
        "120",
    ]


_VERIFY_STATUS_RE = re.compile(r"^VERIFY_STATUS\s+(.*)$")


def _parse_verify_status_line(line: str) -> dict[str, str]:
    """Parse `VERIFY_STATUS key=value ...` lines from verify_fhir_download.py stderr."""

    m = _VERIFY_STATUS_RE.match(line.strip())
    if not m:
        return {}
    rest = m.group(1)
    out: dict[str, str] = {}
    for token in rest.split():
        if "=" not in token:
            continue
        k, v = token.split("=", 1)
        out[k] = v
    return out


def _run_verify_one_resource_type(
    *,
    resource_type: str,
    ndjson_dir: Path,
    fhir_url: str,
    allow_delta: int,
) -> tuple[int, dict[str, str]]:
    cmd = _build_verify_cmd(
        ndjson_dir=ndjson_dir,
        fhir_url=fhir_url,
        allow_delta=int(allow_delta),
        resource_type=resource_type,
    )
    cp = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)

    # verify_fhir_download prints status lines to stderr.
    status_fields: dict[str, str] = {}
    for raw in (cp.stderr or "").splitlines():
        parsed = _parse_verify_status_line(raw)
        if parsed.get("resource_type") == resource_type:
            status_fields = parsed

    # Echo verifier output for operator visibility.
    if cp.stderr:
        for ln in cp.stderr.splitlines():
            print(f"[verify:{resource_type}][ERR] {ln}")
    if cp.stdout:
        for ln in cp.stdout.splitlines():
            print(f"[verify:{resource_type}] {ln}")

    return int(cp.returncode), status_fields


def _stream_prefixed_lines(*, prefix: str, stream, is_stderr: bool) -> None:
    # Read lines until EOF.
    for line in iter(stream.readline, ""):
        line = line.rstrip("\n")
        if not line:
            continue
        if is_stderr:
            print(f"[{prefix}][ERR] {line}")
        else:
            print(f"[{prefix}] {line}")


def _run_one_resource_type(*, resource_type: str, cmd: list[str]) -> int:
    # Note: we keep stdout/stderr separate so we can mark stderr lines.
    env = dict(os.environ)
    env.setdefault("PYTHONUNBUFFERED", "1")
    p = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
        env=env,
    )

    with _RUNNING_PROCS_LOCK:
        _RUNNING_PROCS[resource_type] = p

    assert p.stdout is not None
    assert p.stderr is not None

    t_out = threading.Thread(
        target=_stream_prefixed_lines,
        kwargs={"prefix": resource_type, "stream": p.stdout, "is_stderr": False},
        daemon=True,
    )
    t_err = threading.Thread(
        target=_stream_prefixed_lines,
        kwargs={"prefix": resource_type, "stream": p.stderr, "is_stderr": True},
        daemon=True,
    )

    try:
        t_out.start()
        t_err.start()
        rc = p.wait()
        return int(rc)
    finally:
        # Ensure any final buffered lines are printed.
        t_out.join(timeout=2)
        t_err.join(timeout=2)
        try:
            p.stdout.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            p.stderr.close()
        except Exception:  # noqa: BLE001
            pass
        with _RUNNING_PROCS_LOCK:
            _RUNNING_PROCS.pop(resource_type, None)


def _terminate_running_processes() -> None:
    """Best-effort termination of any child downloads.

    We start each child in a new session (process group) so we can terminate the
    entire tree.
    """

    with _RUNNING_PROCS_LOCK:
        items = list(_RUNNING_PROCS.items())

    if not items:
        return

    print("\nTerminating running download subprocesses...")

    # First pass: SIGTERM
    for rt, p in items:
        if p.poll() is not None:
            continue
        try:
            os.killpg(p.pid, signal.SIGTERM)
            print(f"  [{rt}] sent SIGTERM")
        except Exception:  # noqa: BLE001
            try:
                p.terminate()
                print(f"  [{rt}] sent terminate()")
            except Exception:  # noqa: BLE001
                pass

    # Wait briefly
    deadline = time.time() + 5
    while time.time() < deadline:
        still = False
        for _, p in items:
            if p.poll() is None:
                still = True
                break
        if not still:
            return
        time.sleep(0.1)

    # Second pass: SIGKILL
    for rt, p in items:
        if p.poll() is not None:
            continue
        try:
            os.killpg(p.pid, signal.SIGKILL)
            print(f"  [{rt}] sent SIGKILL")
        except Exception:  # noqa: BLE001
            try:
                p.kill()
                print(f"  [{rt}] sent kill()")
            except Exception:  # noqa: BLE001
                pass


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Kept as a separate function so we can unit test defaults/flags without
    invoking network/download behavior.
    """

    parser = argparse.ArgumentParser(
        prog="download_cms_ndjson.py",
        description="Download FHIR resources from CMS API to NDJSON files",
    )

    parser.add_argument(
        "output_dir",
        help="Output directory for NDJSON files (required)"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Page size for FHIR API requests (default: 1000)"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        dest="stop_after_this_many",
        help="Stop after downloading this many resources (for testing)"
    )

    parser.add_argument(
        "--resource-types",
        default=None,
        help="Comma-separated list of resource types (default: all supported types)"
    )

    parser.add_argument(
        "--verify-allow-delta",
        type=int,
        default=1000,
        help=(
            "Allow expected_total and unique_id_count to differ by this amount before marking verification as FAIL "
            "(default: 1000)"
        ),
    )

    parser.add_argument(
        "--max-redownload-attempts",
        type=int,
        default=0,
        help=(
            "Max times to re-download a failing resource type (default: 0). "
            "NOTE: re-download is destructive (it deletes local download_state and overwrites the NDJSON)."
        ),
    )

    parser.add_argument(
        "--cms-url",
        default=None,
        help="Override CMS FHIR server URL (default: https://dev.cnpd.internal.cms.gov/fhir/)"
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""

    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # Load environment variables from .env
    load_dotenv(override=True)

    # Determine output directory
    # Intentionally require the output directory to be explicitly provided.
    # This script should fail fast if the caller hasn't decided where files go.
    output_dir = Path(args.output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get CMS FHIR server URL - ALWAYS use CMS server, not test server
    # Priority: 1. Command line arg, 2. CMS_FHIR_URL env var, 3. Hardcoded CMS default
    # Explicitly ignore FHIR_SERVER_URL to avoid using test server
    if args.cms_url:
        fhir_url = args.cms_url
    else:
        fhir_url = os.getenv('CMS_FHIR_URL') or CMS_FHIR_URL

    # Require authentication credentials
    username = _require_env(name='FHIR_API_USERNAME')
    password = _require_env(name='FHIR_API_PASSWORD')

    # Safety check: warn if we're about to use test server
    if 'ndh-server.fast.hl7.org' in fhir_url.lower():
        print("⚠️  WARNING: You're about to download from the TEST server!")
        print(f"    URL: {fhir_url}")
        print("    This is NOT the CMS internal server.")
        print(f"    Expected CMS URL: {CMS_FHIR_URL}")
        print("\nTo use the CMS server, either:")
        print("  1. Don't set FHIR_SERVER_URL in .env (or set CMS_FHIR_URL)")
        print("  2. Use --cms-url flag")
        print("\nContinuing in 3 seconds...")
        import time
        time.sleep(3)

    # Print configuration
    print("\n" + "=" * 80)
    print("CMS FHIR NDJSON Downloader")
    print("=" * 80)
    print(f"FHIR Server:    {fhir_url}")
    print(f"Output Dir:     {output_dir}")
    print(f"Username:       {username}")
    # Never print the real password.
    print(f"Password:       {'*' * len(password)}")
    print(f"Page Size:      {args.count}")
    if args.stop_after_this_many:
        print(f"Limit:          {args.stop_after_this_many} resources (testing mode)")
    if args.resource_types:
        print(f"Resource Types: {args.resource_types}")
    print("=" * 80)
    print()

    # Parse resource types.
    resource_types = _parse_resource_types(args.resource_types)

    print("Executing download commands (serial, one resource type at a time):")
    print(
        "(Each type is verified after download; failing types are NOT re-downloaded by default. "
        "To enable destructive re-downloads, pass --max-redownload-attempts >= 1.)"
    )
    print("\n" + "=" * 80)
    print()

    results: dict[str, int] = {}
    verify_summaries: dict[str, dict[str, str]] = {}
    try:
        extra_redownload_attempts = max(int(args.max_redownload_attempts), 0)
        allow_redownload = extra_redownload_attempts > 0

        for rt in resource_types:
            # Skip download if file already exists and verifies.
            ndjson_path = output_dir / _snake_file_name(rt)

            def _download_once() -> int:
                cmd = _build_create_ndjson_cmd(
                    fhir_url=fhir_url,
                    output_dir=output_dir,
                    count=int(args.count),
                    stop_after_this_many=args.stop_after_this_many,
                    resource_type=rt,
                )
                print(f"\n[{rt}] Download command: " + " ".join(cmd))
                return int(_run_one_resource_type(resource_type=rt, cmd=cmd))

            # Verify first if a file exists.
            if ndjson_path.exists() and ndjson_path.stat().st_size > 0:
                v_rc, v_fields = _run_verify_one_resource_type(
                    resource_type=rt,
                    ndjson_dir=output_dir,
                    fhir_url=fhir_url,
                    allow_delta=int(args.verify_allow_delta),
                )
                verify_summaries[rt] = v_fields
                if v_rc == 0 and v_fields.get("status") == "PASS":
                    print(f"[{rt}] Already verified - skipping download.")
                    results[rt] = 0
                    continue

                # Existing file failed verification.
                # IMPORTANT: do NOT auto-redownload or delete the (possibly mostly-correct)
                # local download unless the operator explicitly enabled it.
                if not allow_redownload:
                    print(
                        f"[{rt}] Existing NDJSON failed verification. "
                        "Leaving current file/state intact and moving on to the next resource type. "
                        "To force a fresh download, re-run with --max-redownload-attempts 1 (or more)."
                    )
                    results[rt] = 2
                    continue

            # Download + verify loop.
            max_attempts = extra_redownload_attempts + 1
            last_rc = 99
            last_v_rc = 2
            last_v_fields: dict[str, str] = {}
            for attempt in range(1, max_attempts + 1):
                print(f"[{rt}] Download attempt {attempt}/{max_attempts}...")
                if attempt > 1:
                    print(f"[{rt}] Resetting local download state before fresh re-download...")
                    _reset_resource_download_state(output_dir=output_dir, resource_type=rt)
                last_rc = _download_once()
                if last_rc != 0:
                    print(f"[{rt}][ERR] download subprocess failed rc={last_rc}")

                last_v_rc, last_v_fields = _run_verify_one_resource_type(
                    resource_type=rt,
                    ndjson_dir=output_dir,
                    fhir_url=fhir_url,
                    allow_delta=int(args.verify_allow_delta),
                )
                verify_summaries[rt] = last_v_fields

                if last_v_rc == 0 and last_v_fields.get("status") == "PASS":
                    print(f"[{rt}] Verified PASS.")
                    results[rt] = 0
                    break

                # Re-download is needed if verification did not PASS.
                # (Verifier already encodes the delta threshold via --allow-delta,
                # so FAIL means delta > allow-delta or integrity errors.)
                redownload_needed = True

                if attempt >= max_attempts:
                    results[rt] = 2
                    break

                if redownload_needed:
                    print(
                        f"[{rt}] Verification failed. "
                        f"Re-downloading is enabled; will try again (remaining attempts: {max_attempts - attempt})."
                    )

            # end attempt loop
    except KeyboardInterrupt:
        print("\nInterrupted (Ctrl+C). Terminating running downloads...")
        _terminate_running_processes()
        # Best-effort: return non-zero.
        return 130

    failed = {rt: rc for rt, rc in results.items() if rc != 0}

    print("\n" + "=" * 80)
    print("Download + verify summary")
    print("=" * 80)
    for rt in resource_types:
        rc = results.get(rt, 99)
        status = "OK" if rc == 0 else f"FAILED(rc={rc})"
        print(f"{rt:24s} {status}")

        vf = verify_summaries.get(rt) or {}
        if vf:
            v_status = vf.get("status")
            v_delta = vf.get("delta")
            if v_status or v_delta:
                print(f"{'':24s} verify_status={v_status} delta={v_delta}")

    if not failed:
        print("\n" + "=" * 80)
        print("✅ Download Complete!")
        print("=" * 80)
        print(f"\nNDJSON files saved to: {output_dir}")

        # List downloaded files
        ndjson_files = list(output_dir.glob("*.ndjson"))
        if ndjson_files:
            print(f"\nDownloaded {len(ndjson_files)} file(s):")
            for ndjson_file in sorted(ndjson_files):
                size_mb = ndjson_file.stat().st_size / (1024 * 1024)
                print(f"  📄 {ndjson_file.name} ({size_mb:.2f} MB)")
    else:
        print("\n" + "=" * 80)
        print("❌ Download failed for one or more resource types")
        print("=" * 80)
        # Return the first failure code (deterministic order).
        first_rt = sorted(failed.keys())[0]
        return int(failed[first_rt])

    return 0


if __name__ == "__main__":
    sys.exit(main())
