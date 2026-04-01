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
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    ]
    if stop_after_this_many:
        cmd.extend(["--stop-after-this-many", str(stop_after_this_many)])
    return cmd


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


def main():
    """Main entry point."""
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
        "--cms-url",
        default=None,
        help="Override CMS FHIR server URL (default: https://dev.cnpd.internal.cms.gov/fhir/)"
    )

    args = parser.parse_args()

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
    print(f"Password:       {'*' * len(password)}")
    print(f"Page Size:      {args.count}")
    if args.stop_after_this_many:
        print(f"Limit:          {args.stop_after_this_many} resources (testing mode)")
    if args.resource_types:
        print(f"Resource Types: {args.resource_types}")
    print("=" * 80)
    print()

    # Parse resource types and run one subprocess per resource type in parallel.
    resource_types = _parse_resource_types(args.resource_types)

    cmds: dict[str, list[str]] = {}
    for rt in resource_types:
        cmds[rt] = _build_create_ndjson_cmd(
            fhir_url=fhir_url,
            output_dir=output_dir,
            count=int(args.count),
            stop_after_this_many=args.stop_after_this_many,
            resource_type=rt,
        )

    print("Executing download commands (parallel, one per resource type):")
    for rt in resource_types:
        print(f"  [{rt}] " + " ".join(cmds[rt]))
    print("\n" + "=" * 80)
    print()

    results: dict[str, int] = {}
    try:
        # You asked for one thread per object type every time.
        with ThreadPoolExecutor(max_workers=len(resource_types) or 1) as ex:
            futs = {
                ex.submit(_run_one_resource_type, resource_type=rt, cmd=cmds[rt]): rt
                for rt in resource_types
            }
            for fut in as_completed(futs):
                rt = futs[fut]
                try:
                    results[rt] = int(fut.result())
                except Exception as ex2:  # noqa: BLE001
                    print(f"[{rt}][ERR] ERROR: worker crashed: {type(ex2).__name__}: {ex2}")
                    results[rt] = 99
    except KeyboardInterrupt:
        print("\nInterrupted (Ctrl+C). Terminating running downloads...")
        _terminate_running_processes()
        # Best-effort: return non-zero.
        return 130

    failed = {rt: rc for rt, rc in results.items() if rc != 0}

    print("\n" + "=" * 80)
    print("Parallel download summary")
    print("=" * 80)
    for rt in resource_types:
        rc = results.get(rt, 99)
        status = "OK" if rc == 0 else f"FAILED(rc={rc})"
        print(f"{rt:24s} {status}")

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
        sys.exit(int(failed[first_rt]))

    return 0


if __name__ == "__main__":
    sys.exit(main())
