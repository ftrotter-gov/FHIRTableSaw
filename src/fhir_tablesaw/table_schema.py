from __future__ import annotations

import secrets
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict

import yaml

from fhir_tablesaw.config import ProfileConfig
from fhir_tablesaw.ignore_extensions import IgnoreExtensions
from fhir_tablesaw.naming import column_name_from_path, to_snake
from fhir_tablesaw.ndh_directory import infer_extension_field_name, is_directory_resource
from fhir_tablesaw.stats import ProfileStats
from fhir_tablesaw.stats import NUCC_TAXONOMY_SYSTEM
from fhir_tablesaw.type_inference import reconcile_pg_types


def _entity_to_table(entity: str) -> str:
    # Organization subtype entities are named Organization__pay etc.
    if entity.startswith("Organization__"):
        code = entity.split("__", 1)[1]
        return f"organization_{to_snake(code)}"
    return to_snake(entity)


class TableSchemaEmitter:
    def __init__(self, *, ignore_extensions: IgnoreExtensions):
        self._ignore = ignore_extensions

    def emit(self, *, out_path: Path, cfg: ProfileConfig, stats: ProfileStats) -> None:
        many_pct_threshold = 0.10
        min_many_count = 25

        # NDH Directory mode: only keep a small allowlist of resources
        stats = ProfileStats(
            entities={
                k: v
                for k, v in stats.entities.items()
                if is_directory_resource(k.split("__", 1)[0])
            }
        )

        doc: dict[str, Any] = {
            "version": "0.1",
            "database": "postgres",
            "schema_name": "public",
            "no_foreign_keys": True,
            "postgres_version_min": 15,
            "dedupe": {"unique_nulls_not_distinct": True},
            "tables": {},
        }

        # Emit base entity tables first (we'll mutate them as we decide flattening)
        entity_tables: dict[str, dict[str, Any]] = {}
        for entity, est in stats.entities.items():
            table = _entity_to_table(entity)
            entity_tables[entity] = self._emit_entity_table(entity, table, est)
            doc["tables"][table] = entity_tables[entity]

            # Extensions become COLUMNS (no ext tables). Also detect taxonomy.
            self._apply_extension_flattening(
                entity=entity,
                table_name=table,
                est=est,
                entity_table_doc=entity_tables[entity],
                doc_tables=doc["tables"],
            )

        # Emit child/join tables from arrays (but do NOT create shared fact/component tables).
        for entity, est in stats.entities.items():
            parent_table = _entity_to_table(entity)
            resource_type = entity.split("__", 1)[0]

            for arr_path, a in est.arrays.items():
                if not arr_path.endswith("[]"):
                    continue
                # Skip extension arrays handled separately
                if arr_path.endswith("extension[]"):
                    continue

                comp = arr_path.split(".")[-1].removesuffix("[]")
                comp_snake = to_snake(comp)

                # Reference arrays => join tables to targets
                if "Reference" in a.elem_types:
                    for target_type in sorted(a.reference_targets.keys() or []):
                        target_table = to_snake(target_type)
                        join_table = f"{parent_table}_{target_table}"
                        doc["tables"][join_table] = self._emit_join_table(
                            join_table=join_table,
                            left_table=parent_table,
                            right_table=target_table,
                        )
                    continue

                # Object arrays => flatten / child
                if "object" not in a.elem_types:
                    # Do NOT store jsonb containers; fold as singleton columns via [0] when possible.
                    # For MVP, we skip scalar arrays (or treat as text) to avoid hierarchy.
                    continue

                # Decide flatten vs child table
                should_flatten = (a.pct_many() < many_pct_threshold) and (a.count_many < min_many_count)
                if should_flatten:
                    # inline_first_only
                    for subpath, es in a.element_scalars.items():
                        if "[]" in subpath:
                            continue
                        col = f"{comp_snake}_{to_snake(subpath)}"
                        pg_type = reconcile_pg_types(es.types_seen)
                        entity_tables[entity]["columns"].append(
                            {
                                "name": col,
                                "type": pg_type,
                                "nullable": True,
                                "source": {
                                    "resource": resource_type,
                                    "path": f"{resource_type}.{comp}[0].{subpath}",
                                },
                                "notes": "Flattened from array using inline_first_only",
                            }
                        )
                    continue

                # Child table (parent-owned)
                child_table = f"{parent_table}_{comp_snake}"
                doc["tables"][child_table] = self._emit_child_table(
                    child_table=child_table,
                    parent_table=parent_table,
                    resource_type=resource_type,
                    array_path=arr_path,
                    element_scalars=a.element_scalars,
                )

            # Extensions: handled earlier as columns or taxonomy link tables

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(yaml.safe_dump(doc, sort_keys=False))

    def _emit_entity_table(self, entity: str, table: str, est: Any) -> dict[str, Any]:
        # Scalar columns: include fhir_id always.
        cols = [
            {"name": "id", "type": "bigserial", "nullable": False},
            {
                "name": "fhir_id",
                "type": "text",
                "nullable": False,
                "source": {"resource": entity.split("__", 1)[0], "path": f"{entity.split('__',1)[0]}.id"},
            },
        ]
        unique_constraints = [
            {
                "name": f"{table}_fhir_id_uniq",
                "columns": ["fhir_id"],
                "nulls_not_distinct": True,
            }
        ]

        # Add scalar leaf columns (exclude arrays, references, and extension payload)
        resource_type = entity.split("__", 1)[0]
        for path, ss in est.scalars.items():
            if not path.startswith(resource_type + "."):
                continue
            if path.endswith(".id") and path != f"{resource_type}.id":
                # nested element id: if looks like uuid, call it uuid
                # column name derived from path will include suffix _id; we override to _uuid
                col_name = column_name_from_path(resource_type, path)
                if col_name.endswith("_id"):
                    uuid_col = col_name[:-3] + "_uuid"
                else:
                    uuid_col = col_name + "_uuid"
                pg_type = reconcile_pg_types(ss.types_seen)
                if pg_type == "text":
                    # treat as uuid only if values are uuid-like; we don't have values here,
                    # so keep text. (Stage A can be improved later to record uuid-ness.)
                    pass
                cols.append(
                    {
                        "name": uuid_col,
                        "type": "uuid" if pg_type == "uuid" else "text",
                        "nullable": True,
                        "source": {"resource": resource_type, "path": path},
                    }
                )
                continue
            if path == f"{resource_type}.id":
                continue
            if ".extension[]" in path:
                continue
            # skip reference leafs; represented separately below
            if path.endswith(".reference"):
                continue
            col_name = column_name_from_path(resource_type, path)
            pg_type = reconcile_pg_types(ss.types_seen)
            cols.append(
                {
                    "name": col_name,
                    "type": pg_type,
                    "nullable": True,
                    "source": {"resource": resource_type, "path": path},
                }
            )

        table_doc: dict[str, Any] = {
            "description": f"FHIR {entity} resources.",
            "primary_key": {"strategy": "surrogate", "columns": ["id"]},
            "columns": cols,
            "indexes": [
                {"name": f"{table}_fhir_id_idx", "columns": ["fhir_id"]},
            ],
            "unique_constraints": unique_constraints,
        }

        # Scalar references: add bigint columns when unambiguous
        for ref_path, targets in sorted(est.scalar_reference_targets.items()):
            if not ref_path.startswith(resource_type + "."):
                continue
            field = ref_path[len(resource_type) + 1 :].removesuffix(".reference")
            col_base = to_snake(field)
            if len(targets) == 1:
                target_type = next(iter(targets.keys()))
                target_table = to_snake(target_type)
                col_name = f"{col_base}_id" if not col_base.endswith("_id") else col_base
                table_doc["columns"].append(
                    {
                        "name": col_name,
                        "type": "bigint",
                        "nullable": True,
                        "source": {
                            "resource": resource_type,
                            "path": ref_path,
                            "transform": "resolve_reference_to_surrogate_id",
                            "target_table": target_table,
                            "target_fhir_id_column": "fhir_id",
                        },
                    }
                )
                table_doc["indexes"].append(
                    {"name": f"{table}_{col_name}_idx", "columns": [col_name]}
                )
            else:
                # ambiguous reference targets: store raw reference string
                col_name = f"{col_base}_reference"
                table_doc["columns"].append(
                    {
                        "name": col_name,
                        "type": "text",
                        "nullable": True,
                        "source": {"resource": resource_type, "path": ref_path},
                        "notes": "Ambiguous Reference targets; stored as raw reference string",
                    }
                )

        return table_doc

    def _apply_extension_flattening(
        self,
        *,
        entity: str,
        table_name: str,
        est: Any,
        entity_table_doc: dict[str, Any],
        doc_tables: dict[str, Any],
    ) -> None:
        """Convert extension shapes into either columns on the entity table or taxonomy link tables.

        Key goals:
        - Do not emit *_ext_* tables
        - Do not store extension URLs as data columns
        - Detect NUCC taxonomy (nucc.org/provider-taxonomy) and emit link tables
        """

        # 1) taxonomy link table (per entity table)
        taxonomy_codes: set[str] = set()
        for url, shape in (est.extension_shapes or {}).items():
            taxonomy_codes.update(shape.nucc_taxonomy_codes_seen)

        if taxonomy_codes:
            tax_table = f"{table_name}_taxonomy"
            id_col = f"{table_name}_id"
            if tax_table not in doc_tables:
                doc_tables[tax_table] = {
                    "description": f"NUCC taxonomy codes extracted from extensions for {table_name}.",
                    "primary_key": {"strategy": "surrogate", "columns": ["id"]},
                    "columns": [
                        {"name": "id", "type": "bigserial", "nullable": False},
                        {"name": id_col, "type": "bigint", "nullable": False},
                        {"name": "taxonomy_code", "type": "text", "nullable": False},
                    ],
                    "indexes": [
                        {"name": f"{tax_table}_{id_col}_idx", "columns": [id_col]},
                        {"name": f"{tax_table}_taxonomy_code_idx", "columns": ["taxonomy_code"]},
                    ],
                    "unique_constraints": [
                        {
                            "name": f"{tax_table}_uniq",
                            "columns": [id_col, "taxonomy_code"],
                            "nulls_not_distinct": True,
                        }
                    ],
                }

        # 2) singleton extension fields => columns
        # Only use extensions that appear 0/1 per resource.
        for url, shape in (est.extension_shapes or {}).items():
            if self._ignore.matches(url):
                continue
            if shape.occurrences_max_per_resource > 1:
                # repeating extension: we intentionally ignore/fold only first for now
                continue

            field = infer_extension_field_name(url)

            # Special-case: if extension contains taxonomy system, handled by taxonomy table
            if NUCC_TAXONOMY_SYSTEM in shape.coding_systems:
                continue
            for child_systems in (shape.child_coding_systems or {}).values():
                if NUCC_TAXONOMY_SYSTEM in child_systems:
                    # base-ext-qualification: skip column; taxonomy table covers
                    continue

            # Choose column type.
            # valueString/valueCode => text; valueCodeableConcept => store coding[0].code as text
            if "valueString" in shape.value_kinds or "valueCode" in shape.value_kinds:
                col_type = "text"
            elif "valueCodeableConcept" in shape.value_kinds or any(
                "valueCodeableConcept" in ks for ks in (shape.child_value_kinds or {}).values()
            ):
                col_type = "text"
            else:
                continue

            # Avoid duplicate column name clashes
            existing = {c["name"] for c in entity_table_doc.get("columns") or []}
            col_name = field
            if col_name in existing:
                continue

            entity_table_doc["columns"].append(
                {
                    "name": col_name,
                    "type": col_type,
                    "nullable": True,
                    "notes": f"Flattened from singleton extension {url}",
                }
            )

    def _emit_join_table(self, *, join_table: str, left_table: str, right_table: str) -> dict[str, Any]:
        return {
            "description": f"Join table: {left_table} <-> {right_table} (no FK constraints).",
            "primary_key": {"strategy": "surrogate", "columns": ["id"]},
            "columns": [
                {"name": "id", "type": "bigserial", "nullable": False},
                {"name": f"{left_table}_id", "type": "bigint", "nullable": False},
                {"name": f"{right_table}_id", "type": "bigint", "nullable": False},
            ],
            "indexes": [
                {"name": f"{join_table}_{left_table}_idx", "columns": [f"{left_table}_id"]},
                {"name": f"{join_table}_{right_table}_idx", "columns": [f"{right_table}_id"]},
            ],
            "unique_constraints": [
                {
                    "name": f"{join_table}_uniq",
                    "columns": [f"{left_table}_id", f"{right_table}_id"],
                    "nulls_not_distinct": True,
                }
            ],
        }

    def _emit_child_table(
        self,
        *,
        child_table: str,
        parent_table: str,
        resource_type: str,
        array_path: str,
        element_scalars: Any,
    ) -> dict[str, Any]:
        parent_col = f"{parent_table}_parent_id"
        cols = [
            {"name": "id", "type": "bigserial", "nullable": False},
            {"name": parent_col, "type": "bigint", "nullable": False},
            {"name": "idx", "type": "integer", "nullable": True, "notes": "Ordinal within the parent array"},
        ]

        uniq_cols = [parent_col]

        for subpath, es in sorted(element_scalars.items()):
            if not subpath or "[]" in subpath:
                continue
            col_name = to_snake(subpath)
            pg_type = reconcile_pg_types(es.types_seen)
            cols.append(
                {
                    "name": col_name,
                    "type": pg_type,
                    "nullable": True,
                    "source": {"resource": resource_type, "path": f"{array_path}.{subpath}"},
                }
            )
            uniq_cols.append(col_name)

        return {
            "description": f"Child table derived from {array_path}.",
            "primary_key": {"strategy": "surrogate", "columns": ["id"]},
            "columns": cols,
            "indexes": [
                {"name": f"{child_table}_{parent_col}_idx", "columns": [parent_col]},
            ],
            "unique_constraints": [
                {
                    "name": f"{child_table}_uniq",
                    "columns": uniq_cols,
                    "nulls_not_distinct": True,
                }
            ],
        }

    # NOTE: shared component tables and extension tables intentionally removed in NDH Directory mode.
