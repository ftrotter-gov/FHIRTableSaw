#!/usr/bin/env python3
"""Quick test to verify database connection and schema access."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import os

from sqlalchemy import create_engine, text

from fhir_tablesaw_3tier.env import load_dotenv, require_env


def main():
    """Test database connection."""
    print("=" * 60)
    print("DATABASE CONNECTION TEST")
    print("=" * 60)

    # Load .env
    print("\n1. Loading .env file...")
    load_dotenv()

    # Get connection details
    db_url = require_env("DATABASE_URL")
    schema = os.environ.get("DB_SCHEMA", "fhir_tablesaw")

    print("   ✓ DATABASE_URL loaded")
    print(f"   ✓ DB_SCHEMA: {schema}")

    # Test connection
    print("\n2. Testing database connection...")
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print("   ✓ Connected successfully!")
            print(f"   ✓ PostgreSQL version: {version}")
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        return 1

    # Check if schema exists
    print(f"\n3. Checking if schema '{schema}' exists...")
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"
                ),
                {"schema": schema},
            )
            schema_exists = result.scalar() is not None

            if schema_exists:
                print(f"   ✓ Schema '{schema}' exists")
            else:
                print(f"   ✗ Schema '{schema}' does NOT exist")
    except Exception as e:
        print(f"   ✗ Error checking schema: {e}")
        return 1

    # List tables in schema
    print(f"\n4. Listing tables in schema '{schema}'...")
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                    ORDER BY table_name
                """),
                {"schema": schema},
            )
            tables = [row[0] for row in result]

            if tables:
                print(f"   Found {len(tables)} table(s):")
                for table in tables:
                    print(f"     - {table}")
            else:
                print(f"   No tables found in schema '{schema}'")
    except Exception as e:
        print(f"   ✗ Error listing tables: {e}")
        return 1

    print("\n" + "=" * 60)
    print("CONNECTION TEST SUCCESSFUL!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
