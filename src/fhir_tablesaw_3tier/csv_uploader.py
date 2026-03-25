"""
CSV to PostgreSQL uploader using staging table optimization.

This module bulk uploads CSV files to PostgreSQL using the staging table
approach for optimal performance.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_db_url_from_env() -> str:
    """Get PostgreSQL connection URL from environment variables."""
    import os

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "fhir_tablesaw")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_db_schema_from_env() -> str:
    """Get PostgreSQL schema from environment variables."""
    import os

    return os.getenv("DB_SCHEMA", "public")


class CSVPostgreSQLUploader:
    """Bulk upload CSV files to PostgreSQL using staging table optimization."""

    def __init__(self, *, db_url: str | None = None, schema: str | None = None):
        """Initialize uploader.

        Args:
            db_url: PostgreSQL connection URL (default: from env)
            schema: PostgreSQL schema (default: from env)
        """
        self.db_url = db_url or get_db_url_from_env()
        self.schema = schema or get_db_schema_from_env()
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine (lazy initialization)."""
        if self._engine is None:
            self._engine = create_engine(self.db_url)
        return self._engine

    def upload_csv(
        self,
        *,
        csv_path: str,
        table_name: str,
        if_exists: str = "replace",
        chunk_size: int = 1000,
    ) -> dict[str, Any]:
        """Upload CSV to PostgreSQL using staging table approach.

        Args:
            csv_path: Path to CSV file
            table_name: Target table name
            if_exists: 'replace', 'append', or 'fail'
            chunk_size: Rows per chunk for bulk insert

        Returns:
            Dictionary with upload stats
        """
        csv_file = Path(csv_path)

        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        print("Uploading CSV to PostgreSQL...")
        print(f"  Source: {csv_path}")
        print(f"  Target: {self.schema}.{table_name}")
        print(f"  Mode: {if_exists}")
        print()

        # Read CSV
        print("Reading CSV...")
        df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(df)} rows from CSV")

        if df.empty:
            print("⚠ Warning: CSV file is empty")
            return {
                "status": "empty",
                "table": table_name,
                "rows_uploaded": 0,
                "message": "CSV file is empty",
            }

        # Use staging table approach for optimal performance
        staging_table = f"{table_name}_staging"
        full_table_path = f"{self.schema}.{table_name}"
        full_staging_path = f"{self.schema}.{staging_table}"

        print(f"Loading to staging table: {full_staging_path}")

        # 1. Load to staging table (bulk insert)
        df.to_sql(
            staging_table,
            con=self.engine,
            schema=self.schema,
            if_exists="replace",  # Always replace staging
            index=False,
            method="multi",
            chunksize=chunk_size,
        )

        print(f"✓ Loaded {len(df)} rows to staging table")

        # 2. Transfer from staging to target table
        print(f"Transferring to target table: {full_table_path}")

        with self.engine.begin() as conn:
            if if_exists == "replace":
                # Drop and recreate target table from staging
                conn.execute(text(f"DROP TABLE IF EXISTS {full_table_path}"))
                conn.execute(
                    text(f"CREATE TABLE {full_table_path} AS SELECT * FROM {full_staging_path}")
                )
            elif if_exists == "append":
                # Create target if not exists, then append
                conn.execute(
                    text(
                        f"""
                    CREATE TABLE IF NOT EXISTS {full_table_path} AS
                    SELECT * FROM {full_staging_path} WHERE false
                    """
                    )
                )
                conn.execute(
                    text(f"INSERT INTO {full_table_path} SELECT * FROM {full_staging_path}")
                )
            else:  # fail
                # Check if table exists
                result = conn.execute(
                    text(
                        f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = '{self.schema}'
                        AND table_name = '{table_name}'
                    )
                    """
                    )
                )
                exists = result.scalar()
                if exists:
                    raise ValueError(f"Table {full_table_path} already exists and if_exists='fail'")

                # Create new table from staging
                conn.execute(
                    text(f"CREATE TABLE {full_table_path} AS SELECT * FROM {full_staging_path}")
                )

            # 3. Drop staging table
            conn.execute(text(f"DROP TABLE IF EXISTS {full_staging_path}"))

        print("✓ Data transfer complete!")
        print(f"✓ Uploaded {len(df)} rows to {full_table_path}")

        return {
            "status": "success",
            "table": table_name,
            "full_table_path": full_table_path,
            "rows_uploaded": len(df),
            "if_exists": if_exists,
            "columns": list(df.columns),
        }

    def close(self):
        """Close database connection."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
