#!/usr/bin/env python3
"""
Test script to check PostgreSQL table counts across three schemas
and compare them with ndjson source file line counts.
"""

import os
import sys
from pathlib import Path
import psycopg


def load_env_file(*, env_path='.env'):
    """Load environment variables from .env file."""
    if not os.path.exists(env_path):
        return
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            # Parse KEY=VALUE lines
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                # Only set if not already in environment
                if key and key not in os.environ:
                    os.environ[key] = value


# Load environment variables from .env file
load_env_file()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Schema names
TEST_SCHEMA = os.getenv('TEST_FHIR_SCHEMA')
CMS_SCHEMA = os.getenv('CMS_FHIR_SCHEMA')
PALANTIR_SCHEMA = os.getenv('PALANTIR_FHIR_SCHEMA')

# Source directories
TEST_DIR = os.getenv('TEST_FHIR_DIR')
CMS_DIR = os.getenv('CMS_FHIR_DIR')
PALANTIR_DIR = os.getenv('PALANTIR_FHIR_DIR')


class SchemaTableCounter:
    """Helper class to count tables and rows in PostgreSQL schemas."""
    
    @staticmethod
    def connect_db():
        """Create database connection."""
        conn_string = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
        return psycopg.connect(conn_string)
    
    @staticmethod
    def schema_exists(*, cursor, schema_name):
        """Check if a schema exists."""
        query = """
            SELECT EXISTS(
                SELECT 1 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            );
        """
        cursor.execute(query, (schema_name,))
        return cursor.fetchone()[0]
    
    @staticmethod
    def get_all_schemas(*, cursor):
        """Get list of all schemas in the database."""
        query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name;
        """
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    
    @staticmethod
    def get_tables_in_schema(*, cursor, schema_name):
        """Get list of tables in a schema."""
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        cursor.execute(query, (schema_name,))
        return [row[0] for row in cursor.fetchall()]
    
    @staticmethod
    def count_rows_in_table(*, cursor, schema_name, table_name):
        """Count rows in a specific table."""
        query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";'
        cursor.execute(query)
        return cursor.fetchone()[0]
    
    @staticmethod
    def count_lines_in_file(*, filepath):
        """Count lines in a file using wc -l."""
        if not os.path.exists(filepath):
            return None
        
        import subprocess
        try:
            result = subprocess.run(
                ['wc', '-l', filepath],
                capture_output=True,
                text=True,
                check=True
            )
            # wc -l returns "count filename", so we extract just the count
            count = int(result.stdout.strip().split()[0])
            return count
        except Exception as e:
            print(f"Error counting lines in {filepath}: {e}")
            return None
    
    @staticmethod
    def find_ndjson_files(*, directory):
        """Find all .ndjson files in a directory."""
        if not directory or not os.path.exists(directory):
            return []
        
        ndjson_files = []
        for file in Path(directory).glob('*.ndjson'):
            ndjson_files.append(file)
        return sorted(ndjson_files)


def print_section_header(*, text):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def analyze_schema(*, conn, schema_name, source_dir, schema_label):
    """Analyze a single schema and its corresponding source files."""
    print_section_header(text=f"{schema_label} - Schema: {schema_name}")
    
    cursor = conn.cursor()
    
    # Get tables in schema
    tables = SchemaTableCounter.get_tables_in_schema(cursor=cursor, schema_name=schema_name)
    
    if not tables:
        print(f"⚠️  No tables found in schema '{schema_name}'")
        return
    
    print(f"\nFound {len(tables)} table(s) in schema '{schema_name}':")
    
    # Count rows in each table
    table_counts = {}
    for table in tables:
        count = SchemaTableCounter.count_rows_in_table(
            cursor=cursor,
            schema_name=schema_name,
            table_name=table
        )
        table_counts[table] = count
        print(f"  📊 {table}: {count:,} rows")
    
    # Find and count ndjson files
    print(f"\nSource directory: {source_dir}")
    
    if not source_dir or not os.path.exists(source_dir):
        print(f"⚠️  Source directory does not exist or is not configured")
        cursor.close()
        return
    
    ndjson_files = SchemaTableCounter.find_ndjson_files(directory=source_dir)
    
    if not ndjson_files:
        print(f"⚠️  No .ndjson files found in {source_dir}")
        cursor.close()
        return
    
    print(f"\nFound {len(ndjson_files)} .ndjson file(s):")
    
    file_counts = {}
    for ndjson_file in ndjson_files:
        count = SchemaTableCounter.count_lines_in_file(filepath=str(ndjson_file))
        file_counts[ndjson_file.name] = count
        print(f"  📄 {ndjson_file.name}: {count:,} lines")
    
    # Compare counts
    print("\n🔍 Comparison:")
    
    # Map table names to file names (assuming table names match file names without extension)
    for table_name, table_count in table_counts.items():
        # Look for matching ndjson file
        matching_file = f"{table_name}.ndjson"
        
        if matching_file in file_counts:
            file_count = file_counts[matching_file]
            if table_count == file_count:
                print(f"  ✅ {table_name}: MATCH ({table_count:,} rows)")
            else:
                diff = table_count - file_count
                print(f"  ❌ {table_name}: MISMATCH - Table: {table_count:,}, File: {file_count:,}, Diff: {diff:,}")
        else:
            print(f"  ⚠️  {table_name}: No matching .ndjson file found")
    
    # Check for ndjson files without matching tables
    for file_name in file_counts.keys():
        table_name = file_name.replace('.ndjson', '')
        if table_name not in table_counts:
            print(f"  ⚠️  {file_name}: No matching table found in schema")
    
    cursor.close()


def main():
    """Main function to analyze all three schemas."""
    print("\n🔬 FHIRTableSaw Schema and Source File Count Comparison")
    print(f"Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    try:
        # Connect to database
        conn = SchemaTableCounter.connect_db()
        print("✅ Successfully connected to PostgreSQL")
        
        # Show all available schemas
        cursor = conn.cursor()
        all_schemas = SchemaTableCounter.get_all_schemas(cursor=cursor)
        print(f"\n📋 Available schemas in database: {', '.join(all_schemas)}")
        cursor.close()
        
        # Analyze each schema
        analyze_schema(
            conn=conn,
            schema_name=TEST_SCHEMA,
            source_dir=TEST_DIR,
            schema_label="TEST ENVIRONMENT"
        )
        
        analyze_schema(
            conn=conn,
            schema_name=CMS_SCHEMA,
            source_dir=CMS_DIR,
            schema_label="CMS ENVIRONMENT"
        )
        
        analyze_schema(
            conn=conn,
            schema_name=PALANTIR_SCHEMA,
            source_dir=PALANTIR_DIR,
            schema_label="PALANTIR ENVIRONMENT"
        )
        
        # Close connection
        conn.close()
        print_section_header(text="Analysis Complete")
        
    except psycopg.OperationalError as e:
        print(f"\n❌ Database connection error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
