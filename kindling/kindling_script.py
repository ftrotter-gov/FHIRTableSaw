#!/usr/bin/env python3

import argparse
import json
from typing import Any

import pandas as pd
import requests


def compact(value: Any) -> Any:
    """
    Keep scalars as-is.
    Convert lists/dicts to compact JSON strings so every row fits the same column layout.
    """
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def extract_next_link(bundle: dict) -> str | None:
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def fetch_resources(endpoint: str, limit: int = 1000) -> list[dict]:
    resources: list[dict] = []
    url = endpoint
    session = requests.Session()
    headers = {"Accept": "application/fhir+json, application/json"}

    while url and len(resources) < limit:
        response = session.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        bundle = response.json()

        if bundle.get("resourceType") != "Bundle":
            raise ValueError(f"Endpoint did not return a FHIR Bundle: {url}")

        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                resources.append(resource)
                if len(resources) >= limit:
                    break

        url = extract_next_link(bundle)

    return resources[:limit]


def resources_to_dataframe(resources: list[dict]) -> pd.DataFrame:
    """
    Flatten resources into one dataframe.
    Nested dict fields become dotted columns.
    Lists/dicts that remain in cells are converted to JSON strings.
    """
    if not resources:
        return pd.DataFrame()

    df = pd.json_normalize(resources, sep=".")
    df = df.map(compact)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch up to 1000 resources from one FHIR endpoint and save to one CSV."
    )
    parser.add_argument(
        "endpoint",
        help="FHIR search endpoint, e.g. https://example.org/fhir/Practitioner"
    )
    parser.add_argument(
        "output_csv",
        help="Output CSV file path, e.g. Practitioner.csv"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of resources to export (default: 1000)"
    )
    args = parser.parse_args()

    resources = fetch_resources(args.endpoint, limit=args.limit)
    df = resources_to_dataframe(resources)
    df.to_csv(args.output_csv, index=False)

    print(f"Fetched {len(resources)} resources")
    print(f"Wrote {len(df)} rows and {len(df.columns)} columns to {args.output_csv}")


if __name__ == "__main__":
    main()
