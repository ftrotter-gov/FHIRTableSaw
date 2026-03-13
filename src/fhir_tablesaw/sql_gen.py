from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def _qident(name: str) -> str:
    # conservative quoting for snake_case; still quote to be safe
    return '"' + name.replace('"', '""') + '"'


def _render_column(col: dict[str, Any]) -> str:
    name = _qident(col["name"])
    ctype = col["type"]
    nullable = col.get("nullable", True)
    parts = [name, ctype]
    if not nullable:
        parts.append("NOT NULL")
    # Optional fixed default (we only use this for extension_url currently)
    if "default" in col:
        dv = col["default"]
        if isinstance(dv, str):
            parts.append("DEFAULT " + "'" + dv.replace("'", "''") + "'")
        else:
            parts.append(f"DEFAULT {dv}")
    return " ".join(parts)


def _render_create_table(schema_name: str, table_name: str, tdef: dict[str, Any]) -> str:
    cols = tdef.get("columns") or []
    col_sql = ["  " + _render_column(c) for c in cols]

    # primary key
    pk = (tdef.get("primary_key") or {}).get("columns") or ["id"]
    col_sql.append(f"  CONSTRAINT {_qident(table_name + '_pk')} PRIMARY KEY ({', '.join(_qident(c) for c in pk)})")

    # unique constraints
    for uc in tdef.get("unique_constraints") or []:
        name = uc.get("name") or f"{table_name}_uniq"
        cols = uc.get("columns") or []
        if not cols:
            continue
        nulls_not_distinct = bool(uc.get("nulls_not_distinct", False))
        nn = " NULLS NOT DISTINCT" if nulls_not_distinct else ""
        col_sql.append(
            f"  CONSTRAINT {_qident(name)} UNIQUE{nn} ({', '.join(_qident(c) for c in cols)})"
        )

    full_table = f"{_qident(schema_name)}.{_qident(table_name)}" if schema_name else _qident(table_name)
    sql = [f"CREATE TABLE IF NOT EXISTS {full_table} (", ",\n".join(col_sql), ");", ""]
    return "\n".join(sql)


def _render_indexes(schema_name: str, table_name: str, tdef: dict[str, Any]) -> str:
    full_table = f"{_qident(schema_name)}.{_qident(table_name)}" if schema_name else _qident(table_name)
    stmts: list[str] = []
    for idx in tdef.get("indexes") or []:
        name = idx.get("name")
        cols = idx.get("columns") or []
        if not name or not cols:
            continue
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {_qident(name)} ON {full_table} ({', '.join(_qident(c) for c in cols)});"
        )
    return "\n".join(stmts) + ("\n" if stmts else "")


def generate_create_table_files(*, table_schema_path: Path, out_dir: Path) -> None:
    doc = yaml.safe_load(table_schema_path.read_text())
    schema_name = doc.get("schema_name") or "public"
    tables: dict[str, Any] = doc.get("tables") or {}

    out_dir.mkdir(parents=True, exist_ok=True)
    for table_name, tdef in tables.items():
        create = _render_create_table(schema_name, table_name, tdef)
        indexes = _render_indexes(schema_name, table_name, tdef)

        path = out_dir / f"{table_name}.sql"
        path.write_text(create + indexes)
