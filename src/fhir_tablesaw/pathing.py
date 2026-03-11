from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator


@dataclass(frozen=True)
class PathValue:
    path: str
    value: Any


def _join(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


def walk_paths(*, root_name: str, obj: Any, ignore_meta: bool = True) -> Iterator[PathValue]:
    """Walk an arbitrary FHIR JSON object and yield PathValue records.

    Path format:
    - Objects: Resource.field.subfield
    - Arrays:  Resource.field[]
    - Array elements are traversed as Resource.field[]....

    Notes:
    - This yields leaf scalars and also yields a record for each array container
      (so array cardinality can be tracked).
    """

    def rec(prefix: str, v: Any) -> Iterator[PathValue]:
        if isinstance(v, dict):
            for k, vv in v.items():
                if ignore_meta and prefix == root_name and k == "meta":
                    continue
                yield from rec(_join(prefix, k), vv)
            return
        if isinstance(v, list):
            # record the array container itself
            yield PathValue(path=f"{prefix}[]", value=v)
            for el in v:
                yield from rec(f"{prefix}[]", el)
            return
        yield PathValue(path=prefix, value=v)

    yield from rec(root_name, obj)
