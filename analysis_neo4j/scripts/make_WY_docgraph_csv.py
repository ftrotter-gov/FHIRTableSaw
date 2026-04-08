#!/usr/bin/env python3

import argparse
import os
import sys
import duckdb


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(
        description="Filter DocGraph CSV by NPI list and patient_count threshold using DuckDB."
    )
    parser.add_argument("--docgraph_csv", required=True)
    parser.add_argument("--limit_to_csv", required=True)
    parser.add_argument("--output_csv", required=True)
    parser.add_argument("--npi_col", default="distinct_npi")
    parser.add_argument("--mode", choices=["either", "both"], default="either")
    parser.add_argument(
        "--patient_count_floor",
        type=int,
        default=50,
        help="Only keep rows with patient_count >= this value (default: 50)",
    )

    args = parser.parse_args()

    docgraph_csv = os.path.abspath(os.path.expanduser(args.docgraph_csv))
    limit_to_csv = os.path.abspath(os.path.expanduser(args.limit_to_csv))
    output_csv = os.path.abspath(os.path.expanduser(args.output_csv))

    if not os.path.exists(docgraph_csv):
        print(f"ERROR: docgraph_csv not found: {docgraph_csv}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(limit_to_csv):
        print(f"ERROR: limit_to_csv not found: {limit_to_csv}", file=sys.stderr)
        sys.exit(1)

    output_dir = os.path.dirname(output_csv)
    if output_dir and not os.path.exists(output_dir):
        print(f"ERROR: output directory not found: {output_dir}", file=sys.stderr)
        sys.exit(1)

    match_op = "OR" if args.mode == "either" else "AND"
    floor = args.patient_count_floor

    docgraph_sql = sql_quote(docgraph_csv)
    limit_sql = sql_quote(limit_to_csv)
    output_sql = sql_quote(output_csv)

    con = duckdb.connect()

    try:
        # Load NPI filter set
        con.execute(f"""
            CREATE TEMP TABLE target_npis AS
            SELECT DISTINCT CAST({args.npi_col} AS BIGINT) AS npi
            FROM read_csv_auto({limit_sql}, header=true)
            WHERE {args.npi_col} IS NOT NULL
        """)

        # Filter graph with patient_count threshold
        query = f"""
        COPY (
            SELECT g.*
            FROM read_csv(
                {docgraph_sql},
                header = true,
                columns = {{
                    'from_npi': 'BIGINT',
                    'to_npi': 'BIGINT',
                    'patient_count': 'BIGINT',
                    'transaction_count': 'BIGINT',
                    'average_day_wait': 'DOUBLE',
                    'std_day_wait': 'DOUBLE'
                }}
            ) AS g
            WHERE (
                    g.from_npi IN (SELECT npi FROM target_npis)
                 {match_op}
                    g.to_npi IN (SELECT npi FROM target_npis)
                  )
              AND g.patient_count >= {floor}
        )
        TO {output_sql}
        WITH (HEADER, DELIMITER ',');
        """

        con.execute(query)
        print(f"✅ Filtered graph written to: {output_csv}")
        print(f"Applied patient_count_floor >= {floor}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
