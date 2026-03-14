import argparse
import json
import sys

from fhir_tablesaw_3tier.fhir.practitioner import practitioner_from_fhir_json


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

    args = parser.parse_args(argv)

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

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
