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
from fhir_tablesaw_3tier.env import load_dotenv
from fhir_tablesaw_3tier.ndh_slurp import slurp_to_postgres


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
        default=200,
        help="FHIR paging size (_count)",
    )
    slurp.add_argument(
        "--hard-limit",
        type=int,
        default=None,
        help="Stop after ingesting N resources of each type (debugging)",
    )

    reset = sub.add_parser(
        "reset-db",
        help="Drop all 3-tier tables and recreate them (WIPE DATA)",
    )

    args = parser.parse_args(argv)

    # Load .env once for all commands.
    load_dotenv()

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
        )
        return 0

    if args.cmd == "reset-db":
        reset_db(db_url=None)
        print("Database reset complete (drop_all + create_all)")
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
