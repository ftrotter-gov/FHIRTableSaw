from __future__ import annotations

import argparse
from pathlib import Path

from fhir_tablesaw.profile import run_profile


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fhir-tablesaw")
    sub = p.add_subparsers(dest="cmd", required=True)

    prof = sub.add_parser("profile", help="Profile a FHIR server and emit yaml config files")
    prof.add_argument("--base-url", required=True, help="FHIR base URL (e.g. https://.../fhir)")
    prof.add_argument("--out-dir", default="out", help="Output directory")
    prof.add_argument("--max-resources-per-type", type=int, default=1000)
    prof.add_argument("--page-size", type=int, default=200)
    prof.add_argument("--rate-limit-qps", type=float, default=5.0)
    prof.add_argument("--timeout-seconds", type=float, default=30.0)
    prof.add_argument(
        "--no-require-search-type",
        action="store_false",
        dest="require_search_type",
        help="Include resource types even if CapabilityStatement does not list search-type interaction",
    )
    prof.add_argument("--include", action="append", default=[], help="Resource type allow list")
    prof.add_argument("--exclude", action="append", default=[], help="Resource type deny list")
    prof.add_argument(
        "--bearer-token",
        default=None,
        help="Bearer token (optional). If omitted, unauthenticated requests are used.",
    )
    prof.add_argument(
        "--ignore-extensions",
        default=None,
        help="Path to ignore_extensions.yaml (optional). If missing, a default will be created.",
    )

    sql = sub.add_parser("emit-sql", help="Generate one CREATE TABLE DDL file per table")
    sql.add_argument("--table-schema", required=True, help="Path to table-schema.yaml")
    sql.add_argument("--out-dir", required=True, help="Output directory for per-table SQL files")

    return p


def main() -> None:
    args = build_parser().parse_args()

    if args.cmd == "profile":
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_profile(
            base_url=args.base_url,
            out_dir=out_dir,
            bearer_token=args.bearer_token,
            include_resource_types=args.include,
            exclude_resource_types=args.exclude,
            require_search_type=args.require_search_type,
            max_resources_per_type=args.max_resources_per_type,
            page_size=args.page_size,
            rate_limit_qps=args.rate_limit_qps,
            timeout_seconds=args.timeout_seconds,
            ignore_extensions_path=Path(args.ignore_extensions) if args.ignore_extensions else None,
        )
        return

    if args.cmd == "emit-sql":
        from fhir_tablesaw.sql_gen import generate_create_table_files

        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        generate_create_table_files(
            table_schema_path=Path(args.table_schema),
            out_dir=out_dir,
        )
        return

    raise SystemExit(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
