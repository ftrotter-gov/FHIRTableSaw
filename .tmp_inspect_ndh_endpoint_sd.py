from __future__ import annotations

"""Inspect the NDH Endpoint StructureDefinition.

This utility is intentionally a standalone script (no project imports) so it can
be run safely during development to discover canonical URLs.

It prints:
- The NDH Endpoint profile canonical URL
- Any candidate extension URLs for an endpoint rank extension
"""

import json
import sys
import urllib.request


SD_JSON_URL = "https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-Endpoint.json"


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/fhir+json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def main() -> int:
    sd = fetch_json(SD_JSON_URL)
    print("StructureDefinition.ndh-Endpoint")
    print("  url:", sd.get("url"))
    print("  version:", sd.get("version"))

    snap = (sd.get("snapshot") or {}).get("element") or []

    # Find any elements that mention rank
    rank_elems = []
    for e in snap:
        blob = " ".join(
            [
                str(e.get("id") or ""),
                str(e.get("sliceName") or ""),
                str(e.get("short") or ""),
                str(e.get("definition") or ""),
            ]
        ).lower()
        if "rank" in blob:
            rank_elems.append(e)

    print(f"\nFound {len(rank_elems)} snapshot elements mentioning 'rank'.")
    for e in rank_elems[:30]:
        print(
            "  - id=",
            e.get("id"),
            " path=",
            e.get("path"),
            " sliceName=",
            e.get("sliceName"),
        )
        if e.get("type"):
            profiles = [t.get("profile") for t in (e.get("type") or []) if t.get("profile")]
            if profiles:
                print("    type.profile:", profiles)
        if e.get("fixedUri") or e.get("patternUri"):
            print("    fixed/pattern:", e.get("fixedUri") or e.get("patternUri"))

    # Candidate extension URLs often appear as:
    # - Endpoint.extension:rank.url fixedUri
    # - or as a type.profile referencing a StructureDefinition-... extension
    cand_urls: set[str] = set()
    for e in snap:
        if e.get("path") in ("Endpoint.extension.url", "Extension.url"):
            if e.get("fixedUri"):
                cand_urls.add(str(e["fixedUri"]))
            if e.get("patternUri"):
                cand_urls.add(str(e["patternUri"]))
        if (e.get("id") or "").lower().endswith(".url") and "rank" in (e.get("id") or "").lower():
            if e.get("fixedUri"):
                cand_urls.add(str(e["fixedUri"]))
            if e.get("patternUri"):
                cand_urls.add(str(e["patternUri"]))

    # Add any extension profiles referenced by rank elements
    for e in rank_elems:
        for t in e.get("type") or []:
            for p in t.get("profile") or []:
                cand_urls.add(str(p))

    print("\nCandidate rank-related canonical URLs:")
    if not cand_urls:
        print("  (none found)")
    else:
        for u in sorted(cand_urls):
            print("  -", u)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
