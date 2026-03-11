from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, DefaultDict
from collections import defaultdict

from fhir_tablesaw.ignore_extensions import IgnoreExtensions
from fhir_tablesaw.pathing import walk_paths
from fhir_tablesaw.references import parse_reference
from fhir_tablesaw.type_inference import infer_pg_type


@dataclass
class ArrayStats:
    n_records_seen: int = 0
    count_0: int = 0
    count_1: int = 0
    count_many: int = 0
    elem_types: set[str] = field(default_factory=set)
    # for reference arrays
    reference_targets: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))
    # scalar subpath (relative to array element) -> stats
    element_scalars: DefaultDict[str, ElementScalarStats] = field(
        default_factory=lambda: defaultdict(ElementScalarStats)
    )

    def observe(self, length: int) -> None:
        self.n_records_seen += 1
        if length <= 0:
            self.count_0 += 1
        elif length == 1:
            self.count_1 += 1
        else:
            self.count_many += 1

    def pct_many(self) -> float:
        denom = self.n_records_seen or 1
        return self.count_many / denom


@dataclass
class ScalarStats:
    present_count: int = 0
    types_seen: set[str] = field(default_factory=set)


@dataclass
class ElementScalarStats:
    """Scalar stats for fields inside array elements (relative to the array element)."""

    present_count: int = 0
    types_seen: set[str] = field(default_factory=set)


@dataclass
class EntityStats:
    record_count_sampled: int = 0
    fhir_id_count: int = 0
    fhir_id_uuid_count: int = 0
    arrays: DefaultDict[str, ArrayStats] = field(default_factory=lambda: defaultdict(ArrayStats))
    scalars: DefaultDict[str, ScalarStats] = field(default_factory=lambda: defaultdict(ScalarStats))
    # extension_url -> count
    extensions: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))
    # scalar reference path -> (target_type -> count)
    scalar_reference_targets: DefaultDict[str, DefaultDict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )


@dataclass
class ProfileStats:
    entities: dict[str, EntityStats]


def organization_type_codes(resource: dict[str, Any]) -> list[str]:
    out: set[str] = set()
    for t in resource.get("type", []) or []:
        for coding in t.get("coding", []) or []:
            code = coding.get("code")
            if code:
                out.add(str(code))
    return sorted(out)


class Profiler:
    def __init__(self, *, ignore_extensions: IgnoreExtensions):
        self._ignore = ignore_extensions
        self._entities: DefaultDict[str, EntityStats] = defaultdict(EntityStats)

    def _entity_names_for_resource(self, resource_type: str, resource: dict[str, Any]) -> list[str]:
        # NDH carve-out: Organization subtyping
        if resource_type == "Organization":
            codes = organization_type_codes(resource)
            if codes:
                return [f"Organization__{c}" for c in codes]
            return ["Organization"]
        return [resource_type]

    def consume_resources(self, resource_type: str, resources: list[dict[str, Any]]) -> None:
        for r in resources:
            entities = self._entity_names_for_resource(resource_type, r)
            for entity in entities:
                self._consume_one(entity, resource_type, r)

    def _consume_one(self, entity: str, resource_type: str, r: dict[str, Any]) -> None:
        st = self._entities[entity]
        st.record_count_sampled += 1

        rid = r.get("id")
        if isinstance(rid, str) and rid:
            st.fhir_id_count += 1
            # uuid detection for fhir ids
            # (import locally to avoid import cycles)
            from fhir_tablesaw.type_inference import is_uuid

            if is_uuid(rid):
                st.fhir_id_uuid_count += 1

        # collect extension urls (non-ignored) anywhere in the resource
        self._collect_extensions(st, r)

        # Track per-record presence for scalar paths
        present_scalar_paths: set[str] = set()

        # Track per-record presence for element scalar subpaths within each array
        present_element_scalars: DefaultDict[str, set[str]] = defaultdict(set)

        for pv in walk_paths(root_name=resource_type, obj=r, ignore_meta=True):
            p = pv.path
            v = pv.value

            # Skip ignored extensions everywhere they appear.
            if p.endswith(".extension[]") and isinstance(v, list):
                # this record corresponds to the array itself; we still count cardinality after filtering
                pass

            # Array container record
            if p.endswith("[]") and isinstance(v, list):
                filtered = v
                if p.endswith("extension[]"):
                    filtered = [
                        ext
                        for ext in v
                        if not (
                            isinstance(ext, dict)
                            and self._ignore.matches(str(ext.get("url")) if ext.get("url") else None)
                        )
                    ]
                arr = st.arrays[p]
                arr.observe(len(filtered))
                # try to infer element type
                for el in filtered[:5]:
                    if isinstance(el, dict) and "reference" in el:
                        arr.elem_types.add("Reference")
                        parsed = parse_reference(str(el.get("reference") or ""))
                        if parsed.target_type:
                            arr.reference_targets[parsed.target_type] += 1
                    elif isinstance(el, dict):
                        arr.elem_types.add("object")
                    elif isinstance(el, list):
                        arr.elem_types.add("array")
                    else:
                        arr.elem_types.add(infer_pg_type(el))
                continue

            # Scalar leaf (including null)
            if isinstance(v, (dict, list)):
                continue

            # If this scalar is inside an ignored extension, walk_paths will still emit it.
            # We detect that by looking for `.extension[]` earlier in the path and checking
            # the url at runtime is hard; instead we rely on filtering extension arrays so
            # ignored entries are not traversed when building schemas later.
            # For stats, we keep it simple: skip extension leaf paths entirely if the path
            # contains `.extension[]`.
            if ".extension[]" in p:
                continue

            # If leaf is within an array element, attach to the nearest array container
            if "[]" in p:
                idx = p.rfind("[]")
                container = p[: idx + 2]
                rest = p[idx + 2 :]
                if rest.startswith("."):
                    rest = rest[1:]
                if container.endswith("[]") and rest:
                    present_element_scalars[container].add(rest)
                    es = st.arrays[container].element_scalars[rest]
                    es.types_seen.add(infer_pg_type(v))
                continue

            present_scalar_paths.add(p)
            ss = st.scalars[p]
            ss.types_seen.add(infer_pg_type(v))

            # Special: count reference targets even for scalar Reference objects (not arrays)
            # (This is for paths like PractitionerRole.organization.reference)
            if p.endswith(".reference") and isinstance(v, str):
                parsed = parse_reference(v)
                if parsed.target_type:
                    st.scalar_reference_targets[p][parsed.target_type] += 1

        for p in present_scalar_paths:
            st.scalars[p].present_count += 1

        for container, subpaths in present_element_scalars.items():
            for sp in subpaths:
                st.arrays[container].element_scalars[sp].present_count += 1

    def build_result(self) -> ProfileStats:
        return ProfileStats(entities=dict(self._entities))

    def _collect_extensions(self, st: EntityStats, obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "extension" and isinstance(v, list):
                    for ext in v:
                        if not isinstance(ext, dict):
                            continue
                        url = ext.get("url")
                        # If this extension is ignored, ignore its entire subtree (nested extensions)
                        if isinstance(url, str) and url and self._ignore.matches(url):
                            continue

                        if isinstance(url, str) and url:
                            st.extensions[url] += 1

                        # recurse into nested extension blocks
                        self._collect_extensions(st, ext)
                else:
                    self._collect_extensions(st, v)
        elif isinstance(obj, list):
            for el in obj:
                self._collect_extensions(st, el)
