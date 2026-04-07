#!/usr/bin/env python3
"""
Utility to inspect the first N lines of NDJSON files to understand their structure.

This helps inform the import logic by showing what fields are actually present
in the real data.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List


def inspect_ndjson_file(*, filepath: Path, num_lines: int = 10) -> List[Dict[str, Any]]:
    """
    Read and parse the first N lines of an NDJSON file.
    
    Args:
        filepath: Path to the NDJSON file
        num_lines: Number of lines to read (default: 10)
    
    Returns:
        List of parsed JSON objects
    """
    records = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    records.append(record)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse line {i+1}: {e}", file=sys.stderr)
                    continue
    
    except FileNotFoundError:
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error reading file {filepath}: {e}", file=sys.stderr)
        return []
    
    return records


def print_structure(*, records: List[Dict[str, Any]], resource_type: str) -> None:
    """
    Print a summary of the structure found in the records.
    
    Args:
        records: List of parsed JSON records
        resource_type: The FHIR resource type name
    """
    if not records:
        print(f"No valid records found for {resource_type}")
        return
    
    print(f"\n{'='*60}")
    print(f"{resource_type} - Found {len(records)} sample records")
    print(f"{'='*60}\n")
    
    # Collect all field names from all records
    all_fields = set()
    for record in records:
        all_fields.update(record.keys())
    
    print(f"Top-level fields present: {sorted(all_fields)}\n")
    
    # Show first record as example
    if records:
        print("Example record (first):")
        print(json.dumps(records[0], indent=2)[:1000])  # Limit to first 1000 chars
        if len(json.dumps(records[0], indent=2)) > 1000:
            print("\n... (truncated)")
        print()


def main() -> None:
    """Main entry point for CLI usage."""
    if len(sys.argv) < 2:
        print("Usage: python inspect_ndjson.py <path_to_ndjson_file> [num_lines]")
        print("\nExample:")
        print("  python inspect_ndjson.py /data/Practitioner.ndjson 10")
        sys.exit(1)
    
    filepath = Path(sys.argv[1])
    num_lines = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    # Extract resource type from filename
    resource_type = filepath.stem  # Gets filename without extension
    
    print(f"Inspecting: {filepath}")
    print(f"Reading first {num_lines} lines...\n")
    
    records = inspect_ndjson_file(filepath=filepath, num_lines=num_lines)
    print_structure(records=records, resource_type=resource_type)


if __name__ == "__main__":
    main()
