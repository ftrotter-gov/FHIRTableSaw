from __future__ import annotations

import re


def to_snake(name: str) -> str:
    name = name.replace("[]", "")
    name = name.replace("[0]", "")
    # dot segments to underscores
    name = name.replace(".", "_")
    # camel to snake
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
    name = re.sub(r"__+", "_", name)
    return name.strip("_").lower()


def column_name_from_path(resource_type: str, path: str) -> str:
    """Convert a normalized path like `Organization.name` to a snake_case column.

    Removes the leading resource_type prefix.
    """

    prefix = f"{resource_type}."
    p = path
    if p.startswith(prefix):
        p = p[len(prefix) :]
    return to_snake(p)


def extension_table_name(parent_table: str, url: str | None, *, random_suffix: str) -> str:
    """Generate extension table name per project rule.

    Rule: take last URL path segment, split by '-', take last 2 segments.
    Example: base-ext-contactpoint-availabletime => contactpoint_availabletime
    Table: {parent}_ext_{a}_{b}
    """

    if not url:
        return f"{parent_table}_ext_{random_suffix}"
    slug = url.rstrip("/").split("/")[-1]
    parts = [p for p in slug.split("-") if p]
    if len(parts) >= 2:
        tail = parts[-2:]
        return f"{parent_table}_ext_{to_snake('_'.join(tail))}"
    return f"{parent_table}_ext_{to_snake(slug) or random_suffix}"
