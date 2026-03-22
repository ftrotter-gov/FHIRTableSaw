#!/usr/bin/env python3
"""Export CREATE TABLE statements from SQLAlchemy models.

This script generates one .sql file per table in the sql/create_table directory.
Each file contains the CREATE TABLE statement for PostgreSQL.
"""

import sys
from pathlib import Path

# Add src to path to import our modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from fhir_tablesaw_3tier.db import models  # noqa: F401 - Import to register models
from fhir_tablesaw_3tier.db.base import Base


def dump_postgresql_ddl(*, output_dir: Path) -> None:
    """Generate CREATE TABLE statements for all models.

    Args:
        output_dir: Directory where SQL files will be written.
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use PostgreSQL dialect for DDL generation
    dialect = postgresql.dialect()

    # Get all table objects from metadata
    tables = Base.metadata.tables

    print(f"Exporting {len(tables)} tables to {output_dir}/")

    for table_name, table in sorted(tables.items()):
        # Generate CREATE TABLE DDL
        create_ddl = CreateTable(table).compile(dialect=dialect)
        sql_content = str(create_ddl).strip() + ";\n"

        # Write to file named after the table
        output_file = output_dir / f"{table_name}.sql"
        output_file.write_text(sql_content)
        print(f"  ✓ {output_file.name}")

    print(f"\nSuccessfully exported {len(tables)} CREATE TABLE statements.")


if __name__ == "__main__":
    # Output directory for SQL files
    output_directory = project_root / "sql" / "create_table"

    dump_postgresql_ddl(output_dir=output_directory)
