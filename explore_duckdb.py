#!/usr/bin/env python3
"""Explore DuckDB structure for InLaw tests."""
import duckdb
import json

# Connect to practitioner database
conn = duckdb.connect('../saw_cache/practitioner.duckdb', read_only=True)

print("=" * 60)
print("TABLES AND VIEWS")
print("=" * 60)
tables = conn.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema='main'").fetchall()
for table_name, table_type in tables:
    print(f"{table_type}: {table_name}")

print("\n" + "=" * 60)
print("FHIR_RESOURCES TABLE")
print("=" * 60)
count = conn.execute("SELECT COUNT(*) FROM fhir_resources").fetchone()[0]
print(f"Total records: {count}")

print("\n" + "=" * 60)
print("SAMPLE FHIR JSON STRUCTURE")
print("=" * 60)
sample = conn.execute("SELECT resource FROM fhir_resources LIMIT 1").fetchone()[0]
parsed = json.loads(sample)
print(f"Resource Type: {parsed.get('resourceType')}")
print(f"Keys: {list(parsed.keys())}")

# Check if there's a gender field
if 'gender' in parsed:
    print(f"Gender field exists: {parsed.get('gender')}")

# Check identifier for NPI
if 'identifier' in parsed:
    print(f"Identifiers: {parsed.get('identifier')}")

conn.close()
