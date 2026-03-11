from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

import yaml


DEFAULT_IGNORED = [
    "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-contactpoint-availabletime",
]


@dataclass(frozen=True)
class IgnoreExtensions:
    """Holds patterns for extension.url values to ignore.

    Patterns are matched using fnmatch-style wildcards (e.g., `*`, `?`).
    """

    patterns: tuple[str, ...]

    def matches(self, url: str | None) -> bool:
        if not url:
            return False
        for pat in self.patterns:
            if fnmatchcase(url, pat):
                return True
        return False

    @staticmethod
    def load_or_create_default(path: Path) -> "IgnoreExtensions":
        if path.exists():
            raw = yaml.safe_load(path.read_text())
            if raw is None:
                pats: list[str] = []
            elif isinstance(raw, list):
                pats = [str(x) for x in raw]
            else:
                raise ValueError(f"ignore_extensions.yaml must be a list of strings; got {type(raw)}")
        else:
            pats = list(DEFAULT_IGNORED)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(yaml.safe_dump(pats, sort_keys=False))
        return IgnoreExtensions(patterns=tuple(pats))
