from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from fhir_tablesaw.config import ProfileConfig
from fhir_tablesaw.stats import ProfileStats


class ModelConfigEmitter:
    def emit(
        self,
        *,
        out_path: Path,
        cfg: ProfileConfig,
        capability_statement: dict[str, Any],
        stats: ProfileStats,
    ) -> None:
        doc: dict[str, Any] = {
            "version": "0.1",
            "server": {
                "base_url": cfg.base_url,
                "fhir_version": "R4",
                "auth": {"type": "bearer" if cfg.bearer_token else "none"},
            },
            "discovery": {
                "method": "CapabilityStatement",
                "require_search_type": cfg.require_search_type,
                "capability_statement": {
                    "resourceType": capability_statement.get("resourceType"),
                    "software": capability_statement.get("software"),
                    "implementation": capability_statement.get("implementation"),
                },
            },
            "sampling": {
                "max_resources_per_type": cfg.max_resources_per_type,
                "page_size": cfg.page_size,
                "rate_limit_qps": cfg.rate_limit_qps,
                "timeout_seconds": cfg.timeout_seconds,
            },
            "heuristics": {
                "many_pct_threshold": 0.10,
                "min_many_count": 25,
                "singleton_array_flatten_strategy": "inline_first_only",
                "endpoint_unification": True,
                "organization_subtyping": True,
                "shared_fact_table_detection": "cross_parent_component_name",
            },
            "stats": self._stats_section(stats),
            "decisions": {
                # Stage A currently proposes decisions implicitly via table-schema.yaml.
                # This section exists for future overrides.
                "note": "Stage A proposals are materialized in table-schema.yaml; overrides may be added here later.",
            },
            "overrides": {
                "force_child_table": [],
                "force_join_table": [],
                "force_flatten": [],
                "drop_paths": [],
                "rename_tables": {},
                "rename_columns": {},
                "extra_indexes": {},
            },
        }
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(yaml.safe_dump(doc, sort_keys=False))

    def _stats_section(self, stats: ProfileStats) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for entity, est in stats.entities.items():
            out[entity] = {
                "record_count_sampled": est.record_count_sampled,
                "fhir_id": {
                    "count": est.fhir_id_count,
                    "uuid_count": est.fhir_id_uuid_count,
                },
                "arrays": {
                    p: {
                        "count_0": a.count_0,
                        "count_1": a.count_1,
                        "count_many": a.count_many,
                        "pct_many": a.pct_many(),
                        "elem_types": sorted(a.elem_types),
                        "reference_targets": dict(a.reference_targets) if a.reference_targets else {},
                    }
                    for p, a in est.arrays.items()
                    if p.endswith("[]")
                },
                "scalar_reference_targets": {
                    p: dict(tgt) for p, tgt in est.scalar_reference_targets.items()
                },
                "extensions": dict(est.extensions),
            }
        return out
