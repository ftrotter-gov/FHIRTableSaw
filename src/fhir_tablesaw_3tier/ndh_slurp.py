"""NDH FHIR server slurper.

This module provides a simple ingestion routine:

FHIR Server -> (Bundle paging) -> parse into canonical models -> persist into DB.

Assumes unauthenticated server.
"""

from __future__ import annotations

import json
import os
import traceback
from collections import Counter
from pathlib import Path
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


def _now() -> float:
    import time

    return time.time()


def _progress_line(label: str, *, processed: int, saved: int, failed: int, started_at: float) -> str:
    elapsed = max(_now() - started_at, 0.000001)
    rate = saved / elapsed
    return (
        f"{label}: processed={processed} saved={saved} failed={failed} "
        f"elapsed={elapsed:0.1f}s rate={rate:0.1f}/s"
    )


def _safe_path_component(value: str) -> str:
    """Return a filesystem-safe component.

    We keep letters, numbers, dash, underscore, and dot; everything else becomes underscore.
    """

    if not value:
        return "unknown"
    out = []
    for ch in value:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    s = "".join(out).strip("._")
    return s or "unknown"


def log_failure(
    *,
    log_dir: str | Path,
    fhir_object_type: str,
    fhir_resource_id: str,
    failed_json: dict[str, Any],
    exc: BaseException,
    stage: str,
) -> None:
    """Write failure artifacts for a single resource.

    Creates:
      log/{type}/{id}/failed.json
      log/{type}/{id}/whatwentwrong.log
    """

    base = Path(log_dir)
    safe_type = _safe_path_component(fhir_object_type)
    safe_id = _safe_path_component(fhir_resource_id)
    out_dir = base / safe_type / safe_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Always write the raw resource payload for postmortem.
    (out_dir / "failed.json").write_text(
        json.dumps(failed_json, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )

    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    msg = "\n".join(
        [
            f"stage: {stage}",
            f"type: {fhir_object_type}",
            f"id: {fhir_resource_id}",
            f"exception: {type(exc).__name__}: {exc}",
            "",
            tb,
        ]
    )
    (out_dir / "whatwentwrong.log").write_text(msg, encoding="utf-8")


def _safe_parse(
    parse_fn,
    raw: dict[str, Any],
    *,
    dropped_counter: Counter[str],
    failures_counter: Counter[str],
    label: str,
    fhir_server_url: str,
    log_dir: str | Path,
):
    """Parse a FHIR resource into canonical, capturing dropped-repeat counts.

    Returns canonical object or None if parsing fails.
    """

    try:
        obj, report = parse_fn(raw, fhir_server_url=fhir_server_url)
        dropped_counter.update(report.dropped_counts)
        return obj
    except Exception as ex:  # noqa: BLE001
        failures_counter[label] += 1
        print(f"WARNING: parse failed for {label}: {ex}")

        rid = str(raw.get("id") or "unknown")
        try:
            log_failure(
                log_dir=log_dir,
                fhir_object_type=label,
                fhir_resource_id=rid,
                failed_json=raw,
                exc=ex,
                stage="parse",
            )
        except Exception as log_ex:  # noqa: BLE001
            print(f"WARNING: failed to write failure log for {label}/{rid}: {log_ex}")
        return None


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
    log_dir: str | Path | None = None,
) -> None:
    """Ingest selected NDH resources from server into DB.

    Order matters because PractitionerRole requires Practitioner persisted.

    Current implemented slice:
    - Practitioner
    - ClinicalOrganization (Organization type=prov)
    - PractitionerRole
    - OrganizationAffiliation
    """

    overall_started_at = _now()

    load_dotenv()
    if db_url is None:
        db_url = require_env("DATABASE_URL")

    if log_dir is None:
        # default relative to CWD
        log_dir = os.environ.get("FHIR_TABLESAW_SLURP_LOG_DIR") or "log"

    schema = os.environ.get("DB_SCHEMA") or "fhir_tablesaw"
    engine = create_engine_with_schema(db_url=db_url, schema=schema)
    if create_schema:
        Base.metadata.create_all(engine)

    dropped: Counter[str] = Counter()
    failures: Counter[str] = Counter()

    # progress counters
    processed: Counter[str] = Counter()
    saved: Counter[str] = Counter()
    started_at: dict[str, float] = {}

    def _tick(label: str, *, did_save: bool) -> None:
        if label not in started_at:
            started_at[label] = _now()
        if did_save:
            saved[label] += 1
        # Print every 10 processed (default chatty)
        if processed[label] % 10 == 0:
            print(
                _progress_line(
                    label,
                    processed=processed[label],
                    saved=saved[label],
                    failed=failures[label],
                    started_at=started_at[label],
                )
            )

    with httpx.Client(base_url=fhir_server_url.rstrip("/") + "/", timeout=30.0) as client:
        with Session(engine) as session:
            print(f"Starting slurp from {fhir_server_url} into schema {schema}")

            # Practitioners
            print("--- Practitioners ---")
            for raw in fetch_bundles(
                client=client,
                resource_type="Practitioner",
                count=count,
                hard_limit=hard_limit,
            ):
                processed["Practitioner"] += 1
                p = _safe_parse(
                    practitioner_from_fhir_json,
                    raw,
                    dropped_counter=dropped,
                    failures_counter=failures,
                    label="Practitioner",
                    fhir_server_url=fhir_server_url,
                    log_dir=log_dir,
                )
                if p is not None:
                    try:
                        save_practitioner(session, p)
                        _tick("Practitioner", did_save=True)
                    except Exception as ex:  # noqa: BLE001
                        session.rollback()
                        failures["Practitioner.persist"] += 1
                        _tick("Practitioner", did_save=False)
                        log_failure(
                            log_dir=log_dir,
                            fhir_object_type="Practitioner",
                            fhir_resource_id=str(raw.get("id") or p.resource_uuid),
                            failed_json=raw,
                            exc=ex,
                            stage="persist",
                        )
                else:
                    _tick("Practitioner", did_save=False)

            session.commit()

            # Organizations -> ClinicalOrganization only (prov)
            print("--- Organizations (ClinicalOrganization) ---")
            for raw in fetch_bundles(
                client=client,
                resource_type="Organization",
                count=count,
                hard_limit=hard_limit,
            ):
                processed["ClinicalOrganization"] += 1
                org = _safe_parse(
                    clinical_organization_from_fhir_json,
                    raw,
                    dropped_counter=dropped,
                    failures_counter=failures,
                    label="ClinicalOrganization",
                    fhir_server_url=fhir_server_url,
                    log_dir=log_dir,
                )
                if org is not None:
                    try:
                        save_clinical_organization(session, org)
                        _tick("ClinicalOrganization", did_save=True)
                    except Exception as ex:  # noqa: BLE001
                        session.rollback()
                        failures["ClinicalOrganization.persist"] += 1
                        _tick("ClinicalOrganization", did_save=False)
                        log_failure(
                            log_dir=log_dir,
                            fhir_object_type="ClinicalOrganization",
                            fhir_resource_id=str(raw.get("id") or org.resource_uuid),
                            failed_json=raw,
                            exc=ex,
                            stage="persist",
                        )
                else:
                    _tick("ClinicalOrganization", did_save=False)

            session.commit()

            # PractitionerRole
            print("--- PractitionerRoles ---")
            for raw in fetch_bundles(
                client=client,
                resource_type="PractitionerRole",
                count=count,
                hard_limit=hard_limit,
            ):
                processed["PractitionerRole"] += 1
                role = _safe_parse(
                    practitioner_role_from_fhir_json,
                    raw,
                    dropped_counter=dropped,
                    failures_counter=failures,
                    label="PractitionerRole",
                    fhir_server_url=fhir_server_url,
                    log_dir=log_dir,
                )
                if role is not None:
                    try:
                        save_practitioner_role(session, role)
                        _tick("PractitionerRole", did_save=True)
                    except ValueError as ex:
                        # missing practitioner/org in DB slice
                        session.rollback()
                        failures["PractitionerRole.missing_refs"] += 1
                        _tick("PractitionerRole", did_save=False)
                        log_failure(
                            log_dir=log_dir,
                            fhir_object_type="PractitionerRole",
                            fhir_resource_id=str(raw.get("id") or role.resource_uuid),
                            failed_json=raw,
                            exc=ex,
                            stage="persist",
                        )
                    except Exception as ex:  # noqa: BLE001
                        session.rollback()
                        failures["PractitionerRole.persist"] += 1
                        _tick("PractitionerRole", did_save=False)
                        log_failure(
                            log_dir=log_dir,
                            fhir_object_type="PractitionerRole",
                            fhir_resource_id=str(raw.get("id") or role.resource_uuid),
                            failed_json=raw,
                            exc=ex,
                            stage="persist",
                        )
                else:
                    _tick("PractitionerRole", did_save=False)

            session.commit()

            # OrganizationAffiliation
            print("--- OrganizationAffiliations ---")
            for raw in fetch_bundles(
                client=client,
                resource_type="OrganizationAffiliation",
                count=count,
                hard_limit=hard_limit,
            ):
                processed["OrganizationAffiliation"] += 1
                aff = _safe_parse(
                    organization_affiliation_from_fhir_json,
                    raw,
                    dropped_counter=dropped,
                    failures_counter=failures,
                    label="OrganizationAffiliation",
                    fhir_server_url=fhir_server_url,
                    log_dir=log_dir,
                )
                if aff is not None:
                    try:
                        save_organization_affiliation(session, aff)
                        _tick("OrganizationAffiliation", did_save=True)
                    except Exception as ex:  # noqa: BLE001
                        session.rollback()
                        failures["OrganizationAffiliation.persist"] += 1
                        _tick("OrganizationAffiliation", did_save=False)
                        log_failure(
                            log_dir=log_dir,
                            fhir_object_type="OrganizationAffiliation",
                            fhir_resource_id=str(raw.get("id") or aff.resource_uuid),
                            failed_json=raw,
                            exc=ex,
                            stage="persist",
                        )
                else:
                    _tick("OrganizationAffiliation", did_save=False)

            session.commit()

    # Print dropped report summary
    print("\n--- dropped-repeats summary (all ingested resources) ---")
    if not dropped:
        print("(none)")
    else:
        for path, cnt in sorted(dropped.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"{path}: {cnt}")

    print("\n--- parse/persist failures summary ---")
    if not failures:
        print("(none)")
    else:
        for k, v in sorted(failures.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"{k}: {v}")

    print("\n--- saved counts ---")
    for k in sorted(saved.keys()):
        print(f"{k}: {saved[k]}")

    overall_elapsed = _now() - overall_started_at
    print(f"\nTotal runtime: {overall_elapsed:0.1f}s")
