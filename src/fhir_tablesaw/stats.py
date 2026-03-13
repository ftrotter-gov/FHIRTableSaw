from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, DefaultDict, Iterable
from collections import defaultdict

from fhir_tablesaw.ignore_extensions import IgnoreExtensions
from fhir_tablesaw.pathing import walk_paths
from fhir_tablesaw.references import parse_reference
from fhir_tablesaw.type_inference import infer_pg_type


NUCC_TAXONOMY_SYSTEM = "http://nucc.org/provider-taxonomy"


@dataclass
class ExtensionShapeStats:
    """Stats about a specific absolute extension URL (not child extension keys)."""

    occurrences_max_per_resource: int = 0
    value_kinds: set[str] = field(default_factory=set)  # e.g. valueString/valueCode/valueCodeableConcept/nested
    coding_systems: set[str] = field(default_factory=set)
    # child_url -> set(valueKinds)
    child_value_kinds: DefaultDict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    child_coding_systems: DefaultDict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    nucc_taxonomy_codes_seen: set[str] = field(default_factory=set)


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
    # absolute extension url -> shape stats
    extension_shapes: DefaultDict[str, ExtensionShapeStats] = field(
        default_factory=lambda: defaultdict(ExtensionShapeStats)
    )
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

        # collect extension urls + shapes (non-ignored) anywhere in the resource
        self._collect_extensions(st, r)
        self._collect_extension_shapes_for_resource(st, r)

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

    def _collect_extension_shapes_for_resource(self, st: EntityStats, resource: dict[str, Any]) -> None:
        """Collect per-resource cardinality + value shape stats for absolute-url extensions."""

        instances = list(self._iter_absolute_extension_instances(resource))
        # cardinality per resource
        counts: DefaultDict[str, int] = defaultdict(int)
        for url, _ext in instances:
            counts[url] += 1
        for url, c in counts.items():
            st.extension_shapes[url].occurrences_max_per_resource = max(
                st.extension_shapes[url].occurrences_max_per_resource, c
            )

        for url, ext in instances:
            shape = st.extension_shapes[url]
            self._analyze_extension_instance(shape, ext)

    def _iter_absolute_extension_instances(self, obj: Any) -> Iterable[tuple[str, dict[str, Any]]]:
        """Yield (absolute_url, extension_dict) for any non-ignored extension entries.

        Child extension keys like {"url": "code"} are NOT yielded as separate instances.
        """

        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "extension" and isinstance(v, list):
                    for ext in v:
                        if not isinstance(ext, dict):
                            continue
                        url = ext.get("url")
                        if isinstance(url, str) and url and self._ignore.matches(url):
                            # ignore whole subtree
                            continue
                        if isinstance(url, str) and url.startswith("http"):
                            yield (url, ext)
                        # still recurse to find other absolute extensions deeper
                        yield from self._iter_absolute_extension_instances(ext)
                else:
                    yield from self._iter_absolute_extension_instances(v)
        elif isinstance(obj, list):
            for el in obj:
                yield from self._iter_absolute_extension_instances(el)

    def _analyze_extension_instance(self, shape: ExtensionShapeStats, ext: dict[str, Any]) -> None:
        # direct value[x]
        for k, v in ext.items():
            if not k.startswith("value"):
                continue
            shape.value_kinds.add(k)
            if k == "valueCodeableConcept" and isinstance(v, dict):
                for coding in v.get("coding", []) or []:
                    if not isinstance(coding, dict):
                        continue
                    system = coding.get("system")
                    code = coding.get("code")
                    if isinstance(system, str) and system:
                        shape.coding_systems.add(system)
                    if system == NUCC_TAXONOMY_SYSTEM and isinstance(code, str) and code:
                        shape.nucc_taxonomy_codes_seen.add(code)

        # nested extensions: treat child url as field keys
        nested = ext.get("extension")
        if isinstance(nested, list) and nested:
            shape.value_kinds.add("nested")
            for child in nested:
                if not isinstance(child, dict):
                    continue
                child_url = child.get("url")
                if not isinstance(child_url, str) or not child_url:
                    continue
                for k, v in child.items():
                    if not k.startswith("value"):
                        continue
                    shape.child_value_kinds[child_url].add(k)
                    if k == "valueCodeableConcept" and isinstance(v, dict):
                        for coding in v.get("coding", []) or []:
                            if not isinstance(coding, dict):
                                continue
                            system = coding.get("system")
                            code = coding.get("code")
                            if isinstance(system, str) and system:
                                shape.child_coding_systems[child_url].add(system)
                            if system == NUCC_TAXONOMY_SYSTEM and isinstance(code, str) and code:
                                shape.nucc_taxonomy_codes_seen.add(code)
                # recurse one level deeper (child may itself have nested extensions)
                if isinstance(child.get("extension"), list):
                    self._analyze_extension_instance(shape, child)
