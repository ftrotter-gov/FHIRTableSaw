import argparse
import json
import sys

from fhir_tablesaw_3tier.fhir.practitioner import practitioner_from_fhir_json
from fhir_tablesaw_3tier.fhir.organization_clinical import (
    clinical_organization_from_fhir_json,
)
from fhir_tablesaw_3tier.fhir.organization_affiliation import (
    organization_affiliation_from_fhir_json,
)
from fhir_tablesaw_3tier.fhir.practitioner_role import (
    practitioner_role_from_fhir_json,
)
from fhir_tablesaw_3tier.db.reset import reset_db
from fhir_tablesaw_3tier.env import load_dotenv, require_env
from fhir_tablesaw_3tier.ndh_slurp import slurp_to_postgres
from fhir_tablesaw_3tier.db.engine import create_engine_with_schema


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint.

    Initial scope: parse a Practitioner JSON file into canonical model and print
    a dropped-repeat report to stdout.
    """

    parser = argparse.ArgumentParser(prog="fhir-tablesaw-3tier")
    sub = parser.add_subparsers(dest="cmd", required=True)

    parse_pract = sub.add_parser("parse-practitioner", help="Parse Practitioner JSON")
    parse_pract.add_argument("--input", required=True, help="Path to Practitioner JSON file")
    parse_pract.add_argument(
        "--fhir-server-url",
        required=False,
        default=None,
        help="Base URL of FHIR server (used for reference resolution)",
    )

    parse_org = sub.add_parser(
        "parse-clinical-organization", help="Parse Clinical Organization (FHIR Organization JSON)"
    )
    parse_org.add_argument("--input", required=True, help="Path to Organization JSON file")
    parse_org.add_argument(
        "--fhir-server-url",
        required=False,
        default=None,
        help="Base URL of FHIR server (used for reference resolution)",
    )

    parse_aff = sub.add_parser(
        "parse-organization-affiliation",
        help="Parse OrganizationAffiliation JSON",
    )
    parse_aff.add_argument(
        "--input", required=True, help="Path to OrganizationAffiliation JSON file"
    )
    parse_aff.add_argument(
        "--fhir-server-url",
        required=False,
        default=None,
        help="Base URL of FHIR server (used for reference resolution)",
    )

    parse_role = sub.add_parser(
        "parse-practitioner-role",
        help="Parse PractitionerRole JSON",
    )
    parse_role.add_argument("--input", required=True, help="Path to PractitionerRole JSON file")
    parse_role.add_argument(
        "--fhir-server-url",
        required=False,
        default=None,
        help="Base URL of FHIR server (used for reference resolution)",
    )

    slurp = sub.add_parser(
        "slurp-ndh",
        help="Ingest from an unauthenticated NDH FHIR server into a Postgres DB",
    )
    slurp.add_argument(
        "--fhir-server-url",
        required=True,
        help="Base URL of the NDH FHIR server (e.g. https://example.org/fhir)",
    )
    slurp.add_argument(
        "--no-create-schema",
        action="store_true",
        help="Do not run Base.metadata.create_all()",
    )
    slurp.add_argument(
        "--count",
        type=int,
        default=1000,
        help="FHIR paging size (_count)",
    )

    slurp.add_argument(
        "--commit-every",
        type=int,
        default=5000,
        help="Commit every N saved resources per type (default: 5000). Set 0 to only commit at the end of each type.",
    )
    slurp.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Print progress every N processed resources (default: 1000)",
    )
    slurp.add_argument(
        "--resolve-endpoints",
        action="store_true",
        help="(Slow) During Practitioner parse, perform additional GETs to validate referenced Endpoints.",
    )
    slurp.add_argument(
        "--no-http2",
        action="store_true",
        help="Disable HTTP/2 (enabled by default). Useful for servers/proxies that misbehave.",
    )
    slurp.add_argument(
        "--hard-limit",
        type=int,
        default=None,
        help="Stop after ingesting N resources of each type (debugging)",
    )

    slurp.add_argument(
        "--log-dir",
        default=None,
        help="Directory to write parse/persist failure artifacts (default: ./log or $FHIR_TABLESAW_SLURP_LOG_DIR)",
    )

    reset = sub.add_parser(
        "reset-db",
        help="Drop all 3-tier tables and recreate them (WIPE DATA)",
    )

    db_info = sub.add_parser(
        "db-info",
        help="Show effective DB connection + schema + table counts (reads .env)",
    )

    args = parser.parse_args(argv)

    # Load .env once for all commands.
    # Use override=True so .env is the source-of-truth even if you have shell
    # env vars set from a previous run.
    load_dotenv(override=True)

    if args.cmd == "parse-practitioner":
        raw = json.loads(open(args.input, "r", encoding="utf-8").read())
        practitioner, report = practitioner_from_fhir_json(
            raw, fhir_server_url=args.fhir_server_url
        )

        # Print canonical object (debug)
        print(practitioner.model_dump_json(indent=2, exclude_none=True))

        # Print dropped-repeat report
        print("\n--- dropped-repeats report ---")
        print(report.to_text())
        return 0

    if args.cmd == "parse-clinical-organization":
        raw = json.loads(open(args.input, "r", encoding="utf-8").read())
        org, report = clinical_organization_from_fhir_json(raw, fhir_server_url=args.fhir_server_url)
        print(org.model_dump_json(indent=2, exclude_none=True))
        print("\n--- dropped-repeats report ---")
        print(report.to_text())
        return 0

    if args.cmd == "parse-organization-affiliation":
        raw = json.loads(open(args.input, "r", encoding="utf-8").read())
        aff, report = organization_affiliation_from_fhir_json(
            raw, fhir_server_url=args.fhir_server_url
        )
        print(aff.model_dump_json(indent=2, exclude_none=True))
        print("\n--- dropped-repeats report ---")
        print(report.to_text())
        return 0

    if args.cmd == "parse-practitioner-role":
        raw = json.loads(open(args.input, "r", encoding="utf-8").read())
        role, report = practitioner_role_from_fhir_json(raw, fhir_server_url=args.fhir_server_url)
        print(role.model_dump_json(indent=2, exclude_none=True))
        print("\n--- dropped-repeats report ---")
        print(report.to_text())
        return 0

    if args.cmd == "slurp-ndh":
        slurp_to_postgres(
            fhir_server_url=args.fhir_server_url,
            db_url=None,
            create_schema=not args.no_create_schema,
            count=args.count,
            hard_limit=args.hard_limit,
            log_dir=args.log_dir,
            commit_every=args.commit_every,
            progress_every=args.progress_every,
            resolve_endpoints=bool(args.resolve_endpoints),
            http2=not bool(args.no_http2),
        )
        return 0

    if args.cmd == "reset-db":
        reset_db(db_url=None)
        print("Database reset complete (drop_all + create_all)")
        return 0

    if args.cmd == "db-info":
        import os
        from sqlalchemy import text

        db_url = require_env("DATABASE_URL")
        schema = os.environ.get("DB_SCHEMA") or "fhir_tablesaw"

        engine = create_engine_with_schema(db_url=db_url, schema=schema)
        with engine.connect() as conn:
            search_path = conn.execute(text("show search_path")).scalar_one()
            schema_exists = conn.execute(
                text(
                    "select schema_name from information_schema.schemata where schema_name=:s"
                ),
                {"s": schema},
            ).scalar_one_or_none()
            tables = conn.execute(
                text(
                    "select table_name from information_schema.tables where table_schema=:s order by table_name"
                ),
                {"s": schema},
            ).scalars().all()

        print(f"DATABASE_URL: {db_url}")
        print(f"DB_SCHEMA: {schema}")
        print(f"search_path: {search_path}")
        print(f"schema exists: {bool(schema_exists)}")
        print(f"tables in schema: {len(tables)}")
        if tables:
            for t in tables:
                print(f"- {t}")
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
