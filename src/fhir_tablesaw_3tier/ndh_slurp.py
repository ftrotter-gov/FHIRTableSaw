"""NDH FHIR server slurper.

This module provides a simple ingestion routine:

FHIR Server -> (Bundle paging) -> parse into canonical models -> persist into DB.

Assumes unauthenticated server.
"""

from __future__ import annotations

import os
from collections import Counter
from typing import Any, Iterable

import httpx
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.db.persist_organization_affiliation import save_organization_affiliation
from fhir_tablesaw_3tier.db.persist_organization_clinical import save_clinical_organization
from fhir_tablesaw_3tier.db.persist_practitioner import save_practitioner
from fhir_tablesaw_3tier.db.persist_practitioner_role import save_practitioner_role
from fhir_tablesaw_3tier.fhir.organization_affiliation import organization_affiliation_from_fhir_json
from fhir_tablesaw_3tier.fhir.organization_clinical import clinical_organization_from_fhir_json
from fhir_tablesaw_3tier.fhir.practitioner import practitioner_from_fhir_json
from fhir_tablesaw_3tier.fhir.practitioner_role import practitioner_role_from_fhir_json
from fhir_tablesaw_3tier.env import load_dotenv, require_env
from fhir_tablesaw_3tier.db.engine import create_engine_with_schema


def _iter_bundle_entries(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for entry in bundle.get("entry", []) or []:
        res = entry.get("resource")
        if isinstance(res, dict):
            yield res


def _find_next_link(bundle: dict[str, Any]) -> str | None:
    for link in bundle.get("link", []) or []:
        if link.get("relation") == "next" and link.get("url"):
            return str(link.get("url"))
    return None


def fetch_bundles(
    *,
    client: httpx.Client,
    resource_type: str,
    count: int = 200,
    extra_params: dict[str, str] | None = None,
    hard_limit: int | None = None,
) -> Iterable[dict[str, Any]]:
    """Yield resources of a given type from a FHIR server using search paging."""

    params = {"_count": str(count)}
    if extra_params:
        params.update(extra_params)

    url = f"{resource_type}"
    seen = 0
    while True:
        r = client.get(url, params=params)
        r.raise_for_status()
        bundle = r.json()
        if bundle.get("resourceType") != "Bundle":
            raise ValueError(f"Expected Bundle for {resource_type} search, got {bundle.get('resourceType')}")

        for res in _iter_bundle_entries(bundle):
            yield res
            seen += 1
            if hard_limit is not None and seen >= hard_limit:
                return

        next_url = _find_next_link(bundle)
        if not next_url:
            return

        # After first request, follow absolute next links
        url = next_url
        params = None


def slurp_to_postgres(
    *,
    fhir_server_url: str,
    db_url: str | None = None,
    create_schema: bool = True,
    count: int = 200,
    hard_limit: int | None = None,
) -> None:
    """Ingest selected NDH resources from server into DB.

    Order matters because PractitionerRole requires Practitioner persisted.

    Current implemented slice:
    - Practitioner
    - ClinicalOrganization (Organization type=prov)
    - PractitionerRole
    - OrganizationAffiliation
    """

    load_dotenv()
    if db_url is None:
        db_url = require_env("DATABASE_URL")

    schema = os.environ.get("DB_SCHEMA") or "fhir_tablesaw"
    engine = create_engine_with_schema(db_url=db_url, schema=schema)
    if create_schema:
        Base.metadata.create_all(engine)

    dropped: Counter[str] = Counter()

    with httpx.Client(base_url=fhir_server_url.rstrip("/") + "/", timeout=30.0) as client:
        with Session(engine) as session:
            # Practitioners
            for raw in fetch_bundles(
                client=client,
                resource_type="Practitioner",
                count=count,
                hard_limit=hard_limit,
            ):
                p, report = practitioner_from_fhir_json(raw, fhir_server_url=fhir_server_url)
                dropped.update(report.dropped_counts)
                save_practitioner(session, p)

            session.commit()

            # Organizations -> ClinicalOrganization only (prov)
            for raw in fetch_bundles(
                client=client,
                resource_type="Organization",
                count=count,
                hard_limit=hard_limit,
            ):
                try:
                    org, report = clinical_organization_from_fhir_json(
                        raw, fhir_server_url=fhir_server_url
                    )
                except Exception:
                    # Non-clinical org types will likely fail NPI constraint; skip for now.
                    continue
                dropped.update(report.dropped_counts)
                save_clinical_organization(session, org)

            session.commit()

            # PractitionerRole
            for raw in fetch_bundles(
                client=client,
                resource_type="PractitionerRole",
                count=count,
                hard_limit=hard_limit,
            ):
                role, report = practitioner_role_from_fhir_json(raw, fhir_server_url=fhir_server_url)
                dropped.update(report.dropped_counts)
                try:
                    save_practitioner_role(session, role)
                except ValueError:
                    # missing practitioner/org in DB slice
                    continue

            session.commit()

            # OrganizationAffiliation
            for raw in fetch_bundles(
                client=client,
                resource_type="OrganizationAffiliation",
                count=count,
                hard_limit=hard_limit,
            ):
                aff, report = organization_affiliation_from_fhir_json(
                    raw, fhir_server_url=fhir_server_url
                )
                dropped.update(report.dropped_counts)
                save_organization_affiliation(session, aff)

            session.commit()

    # Print dropped report summary
    print("\n--- dropped-repeats summary (all ingested resources) ---")
    if not dropped:
        print("(none)")
        return
    for path, cnt in sorted(dropped.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{path}: {cnt}")
