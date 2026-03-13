from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fhir_tablesaw.stats import NUCC_TAXONOMY_SYSTEM


def is_directory_resource(resource_type: str) -> bool:
    return resource_type in {
        "Endpoint",
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    }


def is_nucc_taxonomy_system(system: str | None) -> bool:
    return system == NUCC_TAXONOMY_SYSTEM


def last_slug(url: str) -> str:
    return url.rstrip("/").split("/")[-1]


def infer_extension_field_name(url: str) -> str:
    """Infer a stable column name for common NDH base extensions.

    Examples:
    - base-ext-org-description -> description
    - base-ext-verification-status -> verification_status
    - base-ext-qualification -> qualification (but in practice we special-case to taxonomy tables)
    """

    slug = last_slug(url)
    if slug.endswith("org-description"):
        return "description"
    if slug.endswith("verification-status"):
        return "verification_status"
    if slug.endswith("qualification"):
        return "qualification"
    # generic fallback: base-ext-foo-bar -> foo_bar (drop base/ext prefixes)
    parts = [p for p in slug.split("-") if p]
    while parts and parts[0] in {"base", "ext"}:
        parts = parts[1:]
    if parts[:1] == ["base"]:
        parts = parts[1:]
    if parts[:1] == ["ext"]:
        parts = parts[1:]
    # drop leading base/ext if still present
    parts = [p for p in parts if p not in {"base", "ext"}]
    return "_".join(parts[-3:]) if len(parts) > 3 else "_".join(parts)
