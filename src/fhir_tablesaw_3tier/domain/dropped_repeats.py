from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class DroppedRepeatsReport(BaseModel):
    """Tracks counts of dropped repeating elements during flattening."""

    model_config = ConfigDict(extra="forbid")

    dropped_counts: dict[str, int] = Field(default_factory=dict)

    def add(self, path: str, count: int) -> None:
        if count <= 0:
            return
        self.dropped_counts[path] = self.dropped_counts.get(path, 0) + count

    def to_text(self) -> str:
        if not self.dropped_counts:
            return "(none)"
        lines = []
        for k, v in sorted(self.dropped_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"{k}: {v}")
        return "\n".join(lines)
