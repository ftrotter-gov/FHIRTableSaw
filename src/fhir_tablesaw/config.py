from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProfileConfig:
    base_url: str
    bearer_token: str | None
    include_resource_types: list[str]
    exclude_resource_types: list[str]
    require_search_type: bool
    max_resources_per_type: int
    page_size: int
    rate_limit_qps: float
    timeout_seconds: float
