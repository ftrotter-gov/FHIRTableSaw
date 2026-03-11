from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class ParsedReference:
    raw: str
    target_type: str | None
    target_id: str | None


def parse_reference(ref: str) -> ParsedReference:
    """Parse a FHIR Reference.reference string.

    Handles:
    - relative: "Type/id"
    - absolute: "https://host/.../Type/id"
    - fragments/urns: returns (None, None)
    """

    if not ref:
        return ParsedReference(raw=ref, target_type=None, target_id=None)
    if ref.startswith("#") or ref.startswith("urn:"):
        return ParsedReference(raw=ref, target_type=None, target_id=None)

    # Relative
    if "/" in ref and not ref.startswith("http"):
        parts = [p for p in ref.split("/") if p]
        if len(parts) >= 2:
            return ParsedReference(raw=ref, target_type=parts[-2], target_id=parts[-1])
        return ParsedReference(raw=ref, target_type=None, target_id=None)

    # Absolute
    try:
        parsed = urlparse(ref)
        segs = [p for p in parsed.path.split("/") if p]
        if len(segs) >= 2:
            return ParsedReference(raw=ref, target_type=segs[-2], target_id=segs[-1])
    except Exception:
        pass
    return ParsedReference(raw=ref, target_type=None, target_id=None)
