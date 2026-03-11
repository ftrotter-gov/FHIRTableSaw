from __future__ import annotations

from pathlib import Path
from typing import Any

from fhir_tablesaw.config import ProfileConfig
from fhir_tablesaw.fhir_client import FhirClient
from fhir_tablesaw.ignore_extensions import IgnoreExtensions
from fhir_tablesaw.model_config import ModelConfigEmitter
from fhir_tablesaw.stats import Profiler
from fhir_tablesaw.table_schema import TableSchemaEmitter


def run_profile(
    *,
    base_url: str,
    out_dir: Path,
    bearer_token: str | None,
    include_resource_types: list[str],
    exclude_resource_types: list[str],
    require_search_type: bool,
    max_resources_per_type: int,
    page_size: int,
    rate_limit_qps: float,
    timeout_seconds: float,
    ignore_extensions_path: Path | None,
) -> None:
    cfg = ProfileConfig(
        base_url=base_url,
        bearer_token=bearer_token,
        include_resource_types=include_resource_types,
        exclude_resource_types=exclude_resource_types,
        require_search_type=require_search_type,
        max_resources_per_type=max_resources_per_type,
        page_size=page_size,
        rate_limit_qps=rate_limit_qps,
        timeout_seconds=timeout_seconds,
    )

    ignore_path = ignore_extensions_path or (out_dir / "ignore_extensions.yaml")
    ignore = IgnoreExtensions.load_or_create_default(ignore_path)

    client = FhirClient(
        base_url=cfg.base_url,
        bearer_token=cfg.bearer_token,
        rate_limit_qps=cfg.rate_limit_qps,
        timeout_seconds=cfg.timeout_seconds,
    )

    capability = client.get_capability_statement()
    resource_types = client.discover_resource_types(
        capability,
        require_search_type=cfg.require_search_type,
        include_resource_types=cfg.include_resource_types,
        exclude_resource_types=cfg.exclude_resource_types,
    )

    profiler = Profiler(ignore_extensions=ignore)

    for resource_type in resource_types:
        sampled: list[dict[str, Any]] = list(
            client.sample_resources(
                resource_type,
                max_resources=cfg.max_resources_per_type,
                page_size=cfg.page_size,
            )
        )
        profiler.consume_resources(resource_type, sampled)

    stats = profiler.build_result()

    ModelConfigEmitter().emit(
        out_path=out_dir / "model-config.yaml",
        cfg=cfg,
        capability_statement=capability,
        stats=stats,
    )

    TableSchemaEmitter(ignore_extensions=ignore).emit(
        out_path=out_dir / "table-schema.yaml",
        cfg=cfg,
        stats=stats,
    )
