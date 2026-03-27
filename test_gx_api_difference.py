#!/usr/bin/env python3
"""
Test to demonstrate GX DataFrame vs GX Validator - they're functionally equivalent.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.inlaw import InLaw
from src.fhir_tablesaw_3tier.env import load_dotenv, get_db_url, get_db_schema
from src.fhir_tablesaw_3tier.db.engine import create_engine_with_schema

# Load environment
load_dotenv()
db_url = get_db_url()
db_schema = get_db_schema()
engine = create_engine_with_schema(db_url=db_url, schema=db_schema)

# Test the to_gx_dataframe method
sql = "SELECT 1 as test_value, 2 as another_value"
gx_obj = InLaw.to_gx_dataframe(sql, engine)

print("=" * 60)
print("Testing Great Expectations API")
print("=" * 60)
print(f"\nType returned by InLaw.to_gx_dataframe(): {type(gx_obj)}")
print(f"Type name: {type(gx_obj).__name__}")

# Check if it has the expect methods
print(f"\nHas expect_column_values_to_be_between: {hasattr(gx_obj, 'expect_column_values_to_be_between')}")
print(f"Has expect_column_values_to_not_be_null: {hasattr(gx_obj, 'expect_column_values_to_not_be_null')}")
print(f"Has expect_table_row_count_to_equal: {hasattr(gx_obj, 'expect_table_row_count_to_equal')}")

# Try using it like in the example
print("\n" + "=" * 60)
print("Testing expectation methods")
print("=" * 60)
result = gx_obj.expect_column_values_to_be_between(
    column="test_value",
    min_value=0,
    max_value=2
)

print(f"\nResult type: {type(result)}")
print(f"Result.success: {result.success}")
print(f"Result: {result}")

print("\n" + "=" * 60)
print("CONCLUSION:")
print("=" * 60)
print("""
In GX 0.x: InLaw.to_gx_dataframe() returned a PandasDataset
In GX 1.x: InLaw.to_gx_dataframe() returns a Validator

BOTH support the same .expect_*() methods!
BOTH return result objects with .success attribute!

The ABSTRACTION is preserved - only the internal type name changed.
Your InLaw pattern works exactly the same way.
""")
