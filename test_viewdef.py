#!/usr/bin/env python3
"""Test ViewDefinition creation in DuckDB."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.duckdb_helper import DuckDBHelper

# Connect to practitioner DuckDB
conn = DuckDBHelper.get_connection(resource_type='practitioner')

# Create view from ViewDefinition
view_name = DuckDBHelper.create_view_from_viewdef(
    conn=conn,
    viewdef_path='viewdefs/practitioner.json'
)

print(f"Created view: {view_name}")

# Test the view
print("\nQuerying view for counts...")
result = conn.execute(f"SELECT COUNT(*) as total FROM {view_name}").fetchone()
print(f"Total records: {result[0]}")

# Check gender distribution
print("\nGender distribution:")
result = conn.execute(f"SELECT gender, COUNT(*) as count FROM {view_name} GROUP BY gender").fetchall()
for gender, count in result:
    print(f"  {gender}: {count}")

# Check NPI extraction
print("\nSample NPIs:")
result = conn.execute(f"SELECT npi, first_name, last_name FROM {view_name} WHERE npi IS NOT NULL LIMIT 5").fetchall()
for npi, first, last in result:
    print(f"  {npi}: {first} {last}")

conn.close()
print("\n✅ View creation successful!")
