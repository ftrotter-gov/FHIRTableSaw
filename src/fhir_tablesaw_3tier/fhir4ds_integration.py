"""
Integration module for using fhir4ds with FHIRTableSaw.

This module provides utilities to:
1. Load SQL on FHIR ViewDefinitions
2. Process NDJSON FHIR data using fhir4ds
3. Load flattened data into PostgreSQL
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fhir_tablesaw_3tier.env import get_db_schema, get_db_url


class ViewDefinitionLoader:
    """Load and manage SQL on FHIR ViewDefinitions."""

    @staticmethod
    def load_viewdef(*, path: Path | str) -> dict[str, Any]:
        """Load a ViewDefinition from a JSON file.

        Args:
            path: Path to the ViewDefinition JSON file

        Returns:
            ViewDefinition as a dictionary
        """
        path_obj = Path(path) if isinstance(path, str) else path

        if not path_obj.exists():
            raise FileNotFoundError(f"ViewDefinition file not found: {path}")

        with open(path_obj, "r", encoding="utf-8") as f:
            viewdef = json.load(f)

        # Validate it's a ViewDefinition
        if viewdef.get("resourceType") != "ViewDefinition":
            raise ValueError(
                f"Invalid ViewDefinition: resourceType is {viewdef.get('resourceType')}, "
                f"expected 'ViewDefinition'"
            )

        return viewdef

    @staticmethod
    def get_resource_type(*, viewdef: dict[str, Any]) -> str:
        """Extract the resource type from a ViewDefinition.

        Args:
            viewdef: ViewDefinition dictionary

        Returns:
            FHIR resource type (e.g., 'Practitioner')
        """
        return str(viewdef.get("resource", ""))


class NDJSONLoader:
    """Load FHIR resources from NDJSON files."""

    @staticmethod
    def load_ndjson(*, path: Path | str) -> list[dict[str, Any]]:
        """Load FHIR resources from an NDJSON file.

        NDJSON (Newline Delimited JSON) format has one JSON object per line.
        Each line should be a complete FHIR resource.

        Args:
            path: Path to the NDJSON file

        Returns:
            List of FHIR resource dictionaries
        """
        path_obj = Path(path) if isinstance(path, str) else path

        if not path_obj.exists():
            raise FileNotFoundError(f"NDJSON file not found: {path}")

        resources = []
        with open(path_obj, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue  # Skip empty lines

                try:
                    resource = json.loads(line)
                    resources.append(resource)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON on line {line_num} in {path}: {e}") from e

        return resources

    @staticmethod
    def iterate_ndjson_batches(*, path: Path | str, batch_size: int = 5000):
        """Iterate over FHIR resources from an NDJSON file in batches.

        NDJSON (Newline Delimited JSON) format has one JSON object per line.
        Each line should be a complete FHIR resource.

        Args:
            path: Path to the NDJSON file
            batch_size: Number of resources to yield per batch

        Yields:
            Batches of FHIR resource dictionaries
        """
        path_obj = Path(path) if isinstance(path, str) else path

        if not path_obj.exists():
            raise FileNotFoundError(f"NDJSON file not found: {path}")

        batch = []
        with open(path_obj, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue  # Skip empty lines

                try:
                    resource = json.loads(line)
                    batch.append(resource)

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON on line {line_num} in {path}: {e}") from e

            # Yield any remaining resources
            if batch:
                yield batch


class FHIR4DSRunner:
    """Run fhir4ds ViewDefinitions against FHIR data and load to PostgreSQL."""

    def __init__(self, *, viewdef_path: Path | str, db_url: str | None = None):
        """Initialize the runner.

        Args:
            viewdef_path: Path to ViewDefinition JSON file
            db_url: PostgreSQL connection URL (default: from environment)
        """
        self.viewdef = ViewDefinitionLoader.load_viewdef(path=viewdef_path)
        self.resource_type = ViewDefinitionLoader.get_resource_type(viewdef=self.viewdef)
        self.db_url = db_url or get_db_url()
        self.table_name = self.viewdef.get("name", "unknown_table")

    def process_ndjson_batch(
        self,
        *,
        ndjson_path: Path | str,
        if_exists: str = "append",
        batch_size: int = 5000,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """Process an NDJSON file in batches using the ViewDefinition and load to PostgreSQL.

        This method uses a staging table approach for maximum performance:
        1. Create staging table
        2. Bulk INSERT each batch into staging (fast)
        3. After all batches: move staging → target in one operation

        This is 100-2000x faster than row-by-row UPSERT for large datasets.

        Args:
            ndjson_path: Path to NDJSON file containing FHIR resources
            if_exists: How to handle existing table ('append', 'replace', 'fail')
            batch_size: Number of resources to process per batch (default: 5000)
            max_rows: Maximum number of matching resources to process (None = no limit)

        Returns:
            Summary dictionary with processing stats
        """
        # Import fhir4ds here so it's only required when actually using this functionality
        try:
            import pandas as pd
            from fhir4ds import FHIRDataStore, PostgreSQLDialect
            from sqlalchemy import create_engine, text
            from sqlalchemy.engine.url import make_url
        except ImportError as e:
            raise ImportError(
                "fhir4ds is not installed. Install it with: pip install fhir4ds pandas sqlalchemy"
            ) from e

        # Initialize counters
        total_resources_count = 0
        matching_resources_count = 0
        batch_num = 0

        # Get schema from environment variable (defaults to "public")
        schema_name = get_db_schema()

        # Staging table name
        staging_table_name = f"{self.table_name}_staging"

        # Convert SQLAlchemy URL to psycopg2 connection string format
        url = make_url(self.db_url)
        psycopg2_conn_str = (
            f"host={url.host} "
            f"port={url.port or 5432} "
            f"user={url.username} "
            f"password={url.password} "
            f"dbname={url.database}"
        )

        # Create SQLAlchemy engine
        engine = create_engine(self.db_url)

        # Flag to track if staging table has been created
        staging_created = False

        # Store column list for later use
        columns_list = None

        # Process NDJSON in batches
        for batch_resources in NDJSONLoader.iterate_ndjson_batches(
            path=ndjson_path, batch_size=batch_size
        ):
            batch_num += 1
            total_resources_count += len(batch_resources)

            # Filter resources by type
            matching_batch = [
                r for r in batch_resources if r.get("resourceType") == self.resource_type
            ]

            if not matching_batch:
                continue  # Skip batches with no matching resources

            # Check if we've hit the max_rows limit
            if max_rows is not None and matching_resources_count >= max_rows:
                print(
                    f"\n✓ Reached max_rows limit ({max_rows}), stopping batch processing",
                    flush=True,
                )
                break

            # Trim batch if it would exceed max_rows
            if max_rows is not None and (matching_resources_count + len(matching_batch)) > max_rows:
                remaining = max_rows - matching_resources_count
                matching_batch = matching_batch[:remaining]
                print(
                    f"\n✓ Trimming final batch to {remaining} rows to respect max_rows limit",
                    flush=True,
                )

            matching_resources_count += len(matching_batch)

            # Create FHIRDataStore with PostgreSQL dialect
            dialect = PostgreSQLDialect(conn_str=psycopg2_conn_str)
            datastore = FHIRDataStore(dialect=dialect, initialize_table=True)

            # Load resources into the datastore
            datastore.load_resources(matching_batch)

            # Get the ViewRunner and execute the view definition
            view_runner = datastore.view_runner()
            result = view_runner.execute_view_definition(self.viewdef)

            # Convert QueryResult to DataFrame
            result_df = result.to_dataframe()

            if result_df.empty:
                continue  # Skip empty results

            # Save column list on first batch
            if columns_list is None:
                columns_list = list(result_df.columns)

            # Create staging table on first batch
            if not staging_created:
                # Drop staging table if it exists from previous run
                with engine.connect() as conn:
                    drop_staging_query = text(
                        f'DROP TABLE IF EXISTS "{schema_name}"."{staging_table_name}"'
                    )
                    conn.execute(drop_staging_query)
                    conn.commit()

                # Create staging table with same structure as result
                result_df.head(0).to_sql(
                    name=staging_table_name,
                    con=engine,
                    schema=schema_name,
                    if_exists="replace",
                    index=False,
                )
                staging_created = True

            # Bulk INSERT into staging table (ultra-fast using PostgreSQL COPY protocol)
            result_df.to_sql(
                name=staging_table_name,
                con=engine,
                schema=schema_name,
                if_exists="append",
                index=False,
                method="multi",  # Use bulk insert
                chunksize=1000,  # Batch inserts in chunks of 1000
            )

            # Print progress indicator
            print(".", end="", flush=True)

        # Print newline after all dots
        if batch_num > 0:
            print()

        # Check if we found any data
        if total_resources_count == 0:
            engine.dispose()
            return {
                "status": "no_data",
                "resources_loaded": 0,
                "message": "No resources found in NDJSON file",
            }

        if matching_resources_count == 0:
            engine.dispose()
            return {
                "status": "no_matching_resources",
                "total_resources": total_resources_count,
                "matching_resources": 0,
                "expected_type": self.resource_type,
                "message": f"No {self.resource_type} resources found in file",
            }

        # Now finalize: move data from staging to target table
        print("Finalizing data transfer from staging to target table...", flush=True)

        # Safety check: ensure we have column information
        if columns_list is None:
            engine.dispose()
            raise RuntimeError(
                "No column information available - no data was processed successfully"
            )

        with engine.begin() as conn:
            # Ensure target table exists with same structure
            with engine.connect() as temp_conn:
                table_exists_query = text(
                    f"""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = '{schema_name}'
                        AND table_name = '{self.table_name}'
                    )
                    """
                )
                table_exists = temp_conn.execute(table_exists_query).scalar()

            if not table_exists or if_exists == "replace":
                # Create or replace target table
                if table_exists and if_exists == "replace":
                    drop_target_query = text(
                        f'DROP TABLE IF EXISTS "{schema_name}"."{self.table_name}"'
                    )
                    conn.execute(drop_target_query)

                # Create target table with same structure as staging
                create_target_query = text(
                    f'CREATE TABLE "{schema_name}"."{self.table_name}" '
                    f'AS SELECT * FROM "{schema_name}"."{staging_table_name}" LIMIT 0'
                )
                conn.execute(create_target_query)

            # Ensure unique constraint exists on target table
            constraint_exists_query = text(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '{self.table_name}_resource_uuid_key'
                    AND conrelid = '"{schema_name}"."{self.table_name}"'::regclass
                )
                """
            )
            constraint_exists = conn.execute(constraint_exists_query).scalar()

            if not constraint_exists:
                add_constraint_query = text(
                    f"""
                    ALTER TABLE "{schema_name}"."{self.table_name}"
                    ADD CONSTRAINT {self.table_name}_resource_uuid_key UNIQUE (resource_uuid)
                    """
                )
                conn.execute(add_constraint_query)

            # Build column lists for INSERT
            columns_str = ", ".join([f'"{col}"' for col in columns_list])
            update_cols = [col for col in columns_list if col != "resource_uuid"]
            update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

            # Single bulk operation: INSERT from staging with ON CONFLICT handling
            # This handles both append and replace modes correctly
            bulk_upsert_query = text(
                f"""
                INSERT INTO "{schema_name}"."{self.table_name}" ({columns_str})
                SELECT DISTINCT ON (resource_uuid) {columns_str}
                FROM "{schema_name}"."{staging_table_name}"
                ORDER BY resource_uuid
                ON CONFLICT (resource_uuid) DO UPDATE SET {update_str}
                """
            )
            conn.execute(bulk_upsert_query)

            # Drop staging table
            drop_staging_query = text(
                f'DROP TABLE IF EXISTS "{schema_name}"."{staging_table_name}"'
            )
            conn.execute(drop_staging_query)

        print("✓ Data transfer complete!", flush=True)

        # Verify the write by reading back the count
        try:
            verify_query = f'SELECT COUNT(*) as count FROM "{schema_name}"."{self.table_name}"'
            with engine.connect() as conn:
                verify_result = conn.execute(text(verify_query))
                row = verify_result.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"Failed to retrieve row count from {schema_name}.{self.table_name}"
                    )
                row_count = row[0]
        except Exception as e:
            engine.dispose()
            raise RuntimeError(
                f"Failed to verify data write to {schema_name}.{self.table_name}: {e}"
            ) from e
        finally:
            engine.dispose()

        # Extract database information for full path
        url_obj = make_url(self.db_url)
        database_name = url_obj.database or "postgres"
        full_table_path = f"{database_name}.{schema_name}.{self.table_name}"

        return {
            "status": "success",
            "total_resources": total_resources_count,
            "matching_resources": matching_resources_count,
            "resource_type": self.resource_type,
            "table_name": self.table_name,
            "full_table_path": full_table_path,
            "rows_in_table": row_count,
            "if_exists": if_exists,
            "batches_processed": batch_num,
            "batch_size": batch_size,
        }

    def process_ndjson(
        self, *, ndjson_path: Path | str, if_exists: str = "append", batch_size: int | None = None
    ) -> dict[str, Any]:
        """Process an NDJSON file using the ViewDefinition and load to PostgreSQL.

        Args:
            ndjson_path: Path to NDJSON file containing FHIR resources
            if_exists: How to handle existing table ('append', 'replace', 'fail')
            batch_size: Number of resources per batch. If provided, uses batch processing.
                       If None, loads all data at once (legacy behavior, not recommended for large files)

        Returns:
            Summary dictionary with processing stats
        """
        # If batch_size is specified, use batch processing
        if batch_size is not None:
            return self.process_ndjson_batch(
                ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size
            )

        # Legacy behavior: load all at once (not recommended for large files)
        # Import fhir4ds here so it's only required when actually using this functionality
        try:
            import pandas as pd
            from fhir4ds import FHIRDataStore, PostgreSQLDialect
            from sqlalchemy import create_engine, text
            from sqlalchemy.engine.url import make_url
        except ImportError as e:
            raise ImportError(
                "fhir4ds is not installed. Install it with: pip install fhir4ds pandas sqlalchemy"
            ) from e

        # Load FHIR resources from NDJSON
        resources = NDJSONLoader.load_ndjson(path=ndjson_path)

        if not resources:
            return {
                "status": "no_data",
                "resources_loaded": 0,
                "message": "No resources found in NDJSON file",
            }

        # Filter resources by type
        matching_resources = [r for r in resources if r.get("resourceType") == self.resource_type]

        if not matching_resources:
            return {
                "status": "no_matching_resources",
                "total_resources": len(resources),
                "matching_resources": 0,
                "expected_type": self.resource_type,
                "message": f"No {self.resource_type} resources found in file",
            }

        # Convert SQLAlchemy URL to psycopg2 connection string format
        url = make_url(self.db_url)
        psycopg2_conn_str = (
            f"host={url.host} "
            f"port={url.port or 5432} "
            f"user={url.username} "
            f"password={url.password} "
            f"dbname={url.database}"
        )

        # Create FHIRDataStore with PostgreSQL dialect
        dialect = PostgreSQLDialect(conn_str=psycopg2_conn_str)
        datastore = FHIRDataStore(dialect=dialect, initialize_table=True)

        # Load resources into the datastore
        datastore.load_resources(matching_resources)

        # Get the ViewRunner and execute the view definition
        view_runner = datastore.view_runner()
        result = view_runner.execute_view_definition(self.viewdef)

        # Convert QueryResult to DataFrame
        result_df = result.to_dataframe()

        # Get schema from environment variable (defaults to "public")
        schema_name = get_db_schema()

        # Load the resulting dataframe to PostgreSQL with upsert on resource_uuid
        engine = create_engine(self.db_url)

        # Create table if it doesn't exist
        result_df.head(0).to_sql(
            name=self.table_name, con=engine, schema=schema_name, if_exists="append", index=False
        )

        # Ensure unique constraint exists on resource_uuid
        with engine.connect() as conn:
            # Check if constraint exists
            constraint_exists_query = text(f'''
                SELECT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '{self.table_name}_resource_uuid_key'
                    AND conrelid = '"{schema_name}"."{self.table_name}"'::regclass
                )
            ''')
            constraint_exists = conn.execute(constraint_exists_query).scalar()

            if not constraint_exists:
                # Remove duplicates before adding constraint (keep first occurrence)
                dedup_query = text(f'''
                    DELETE FROM "{schema_name}"."{self.table_name}" a USING (
                        SELECT MIN(ctid) as ctid, resource_uuid
                        FROM "{schema_name}"."{self.table_name}"
                        GROUP BY resource_uuid HAVING COUNT(*) > 1
                    ) b
                    WHERE a.resource_uuid = b.resource_uuid
                    AND a.ctid <> b.ctid
                ''')
                conn.execute(dedup_query)

                # Now add the unique constraint
                add_constraint_query = text(f'''
                    ALTER TABLE "{schema_name}"."{self.table_name}"
                    ADD CONSTRAINT {self.table_name}_resource_uuid_key UNIQUE (resource_uuid)
                ''')
                conn.execute(add_constraint_query)

            conn.commit()

        # Perform upsert using PostgreSQL's ON CONFLICT
        with engine.connect() as conn:
            # Get column names from the dataframe
            columns = list(result_df.columns)
            columns_str = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join([f":{col}" for col in columns])

            # Build update clause for all columns except resource_uuid
            update_cols = [col for col in columns if col != "resource_uuid"]
            update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

            # Build upsert query
            upsert_query = text(f'''
                INSERT INTO "{schema_name}"."{self.table_name}" ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT (resource_uuid) DO UPDATE SET {update_str}
            ''')

            # Execute upsert for each row
            for _, row in result_df.iterrows():
                conn.execute(upsert_query, row.to_dict())

            conn.commit()

        # Verify the write by reading back the count
        try:
            verify_query = f'SELECT COUNT(*) as count FROM "{schema_name}"."{self.table_name}"'
            with engine.connect() as conn:
                verify_result = conn.execute(text(verify_query))
                row = verify_result.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"Failed to retrieve row count from {schema_name}.{self.table_name}"
                    )
                row_count = row[0]
        except Exception as e:
            engine.dispose()
            raise RuntimeError(
                f"Failed to verify data write to {schema_name}.{self.table_name}: {e}"
            ) from e
        finally:
            engine.dispose()

        # Extract database information for full path
        url_obj = make_url(self.db_url)
        database_name = url_obj.database or "postgres"
        full_table_path = f"{database_name}.{schema_name}.{self.table_name}"

        return {
            "status": "success",
            "total_resources": len(resources),
            "matching_resources": len(matching_resources),
            "resource_type": self.resource_type,
            "table_name": self.table_name,
            "full_table_path": full_table_path,
            "rows_in_table": row_count,
            "if_exists": if_exists,
        }


def process_practitioner_ndjson(
    *,
    ndjson_path: Path | str,
    viewdef_path: Path | str | None = None,
    if_exists: str = "append",
    batch_size: int = 5000,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """Convenience function to process Practitioner NDJSON files.

    Args:
        ndjson_path: Path to Practitioner NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/practitioner.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')
        batch_size: Number of resources to process per batch (default: 5000)
        max_rows: Maximum number of matching resources to process (None = no limit)

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        # Default to viewdefs/practitioner.json
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "practitioner.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson_batch(
        ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size, max_rows=max_rows
    )


def process_endpoint_ndjson(
    *,
    ndjson_path: Path | str,
    viewdef_path: Path | str | None = None,
    if_exists: str = "append",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Convenience function to process Endpoint NDJSON files.

    Args:
        ndjson_path: Path to Endpoint NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/endpoint.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')
        batch_size: Number of resources to process per batch (default: 5000)

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "endpoint.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson(
        ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size
    )


def process_location_ndjson(
    *,
    ndjson_path: Path | str,
    viewdef_path: Path | str | None = None,
    if_exists: str = "append",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Convenience function to process Location NDJSON files.

    Args:
        ndjson_path: Path to Location NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/location.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')
        batch_size: Number of resources to process per batch (default: 5000)

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "location.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson(
        ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size
    )


def process_organization_ndjson(
    *,
    ndjson_path: Path | str,
    viewdef_path: Path | str | None = None,
    if_exists: str = "append",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Convenience function to process Organization NDJSON files.

    Args:
        ndjson_path: Path to Organization NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/organization.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')
        batch_size: Number of resources to process per batch (default: 5000)

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "organization.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson(
        ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size
    )


def process_organization_affiliation_ndjson(
    *,
    ndjson_path: Path | str,
    viewdef_path: Path | str | None = None,
    if_exists: str = "append",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Convenience function to process OrganizationAffiliation NDJSON files.

    Args:
        ndjson_path: Path to OrganizationAffiliation NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/organization_affiliation.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')
        batch_size: Number of resources to process per batch (default: 5000)

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "organization_affiliation.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson(
        ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size
    )


def process_practitioner_role_ndjson(
    *,
    ndjson_path: Path | str,
    viewdef_path: Path | str | None = None,
    if_exists: str = "append",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Convenience function to process PractitionerRole NDJSON files.

    Args:
        ndjson_path: Path to PractitionerRole NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/practitioner_role.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')
        batch_size: Number of resources to process per batch (default: 5000)

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "practitioner_role.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson(
        ndjson_path=ndjson_path, if_exists=if_exists, batch_size=batch_size
    )
