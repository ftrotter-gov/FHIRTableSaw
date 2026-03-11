from __future__ import annotations

from pathlib import Path

import yaml

from fhir_tablesaw.ignore_extensions import IgnoreExtensions
from fhir_tablesaw.stats import Profiler
from fhir_tablesaw.table_schema import TableSchemaEmitter
from fhir_tablesaw.config import ProfileConfig


NDH_AVAILABLETIME_URL = "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-contactpoint-availabletime"


def _emit_table_schema(tmp_path: Path, stats) -> dict:
    cfg = ProfileConfig(
        base_url="https://example.invalid/fhir",
        bearer_token=None,
        include_resource_types=[],
        exclude_resource_types=[],
        require_search_type=True,
        max_resources_per_type=1,
        page_size=1,
        rate_limit_qps=0,
        timeout_seconds=1,
    )
    out_path = tmp_path / "table-schema.yaml"
    TableSchemaEmitter(ignore_extensions=IgnoreExtensions(patterns=tuple([NDH_AVAILABLETIME_URL]))).emit(
        out_path=out_path,
        cfg=cfg,
        stats=stats,
    )
    return yaml.safe_load(out_path.read_text())


def test_ignore_extensions_matches_wildcards() -> None:
    ignore = IgnoreExtensions(patterns=("http://hl7.org/fhir/us/ndh/StructureDefinition/*availabletime",))
    assert ignore.matches(NDH_AVAILABLETIME_URL)
    assert not ignore.matches("http://hl7.org/fhir/us/ndh/StructureDefinition/something-else")


def test_organization_subtyping_creates_independent_tables(tmp_path: Path) -> None:
    ignore = IgnoreExtensions(patterns=(NDH_AVAILABLETIME_URL,))
    p = Profiler(ignore_extensions=ignore)

    org = {
        "resourceType": "Organization",
        "id": "org-1",
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "pay",
                        "display": "Payer",
                    }
                ]
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": "555-1212",
                "extension": [
                    {
                        "url": NDH_AVAILABLETIME_URL,
                        "extension": [
                            {"url": "daysOfWeek", "valueCode": "mon"},
                        ],
                    }
                ],
            }
        ],
    }

    p.consume_resources("Organization", [org])
    stats = p.build_result()

    # Profiler creates subtype entity
    assert "Organization__pay" in stats.entities

    # Ignored extension should not be counted
    assert NDH_AVAILABLETIME_URL not in stats.entities["Organization__pay"].extensions

    schema = _emit_table_schema(tmp_path, stats)
    tables = schema.get("tables", {})

    # Table for subtype exists
    assert "organization_pay" in tables
    # Ignored extension should not produce any extension table with availabletime
    assert not any("availabletime" in t for t in tables.keys())


def test_organization_multiple_type_codes_emit_multiple_subtype_entities(tmp_path: Path) -> None:
    ignore = IgnoreExtensions(patterns=(NDH_AVAILABLETIME_URL,))
    p = Profiler(ignore_extensions=ignore)

    org = {
        "resourceType": "Organization",
        "id": "org-2",
        "type": [
            {"coding": [{"code": "pay"}]},
            {"coding": [{"code": "fac"}]},
        ],
    }

    p.consume_resources("Organization", [org])
    stats = p.build_result()
    assert "Organization__pay" in stats.entities
    assert "Organization__fac" in stats.entities

    schema = _emit_table_schema(tmp_path, stats)
    tables = schema.get("tables", {})
    assert "organization_pay" in tables
    assert "organization_fac" in tables


def test_organization_with_no_type_stays_base_organization(tmp_path: Path) -> None:
    ignore = IgnoreExtensions(patterns=(NDH_AVAILABLETIME_URL,))
    p = Profiler(ignore_extensions=ignore)

    org = {"resourceType": "Organization", "id": "org-3", "name": "No type"}
    p.consume_resources("Organization", [org])
    stats = p.build_result()

    assert "Organization" in stats.entities
    assert not any(k.startswith("Organization__") for k in stats.entities)

    schema = _emit_table_schema(tmp_path, stats)
    tables = schema.get("tables", {})
    assert "organization" in tables
