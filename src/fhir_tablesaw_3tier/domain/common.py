from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class CanonicalBase(BaseModel):
    model_config = ConfigDict(extra="forbid")
