from __future__ import annotations

from pathlib import Path

import yaml

from fhir_tablesaw.sql_gen import generate_create_table_files


def test_generate_create_table_files(tmp_path: Path) -> None:
    schema = {
        "version": "0.1",
        "database": "postgres",
        "schema_name": "public",
        "tables": {
            "thing": {
                "primary_key": {"strategy": "surrogate", "columns": ["id"]},
                "columns": [
                    {"name": "id", "type": "bigserial", "nullable": False},
                    {"name": "a", "type": "text", "nullable": True},
                ],
                "unique_constraints": [
                    {"name": "thing_uniq", "columns": ["a"], "nulls_not_distinct": True}
                ],
                "indexes": [{"name": "thing_a_idx", "columns": ["a"]}],
            }
        },
    }

    in_path = tmp_path / "table-schema.yaml"
    in_path.write_text(yaml.safe_dump(schema, sort_keys=False))

    out_dir = tmp_path / "sql" / "create_table"
    generate_create_table_files(table_schema_path=in_path, out_dir=out_dir)

    out_file = out_dir / "thing.sql"
    assert out_file.exists()
    sql = out_file.read_text()
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "UNIQUE NULLS NOT DISTINCT" in sql
    assert "CREATE INDEX IF NOT EXISTS" in sql
