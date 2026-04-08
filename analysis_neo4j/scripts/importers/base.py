"""
Base importer class with common functionality for all FHIR resource importers.
"""

import json
import sys
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from neo4j import GraphDatabase, Session


class BaseImporter:
    """
    Base class for FHIR NDJSON importers.
    
    Provides common functionality like:
    - Reading NDJSON files
    - Batching
    - Progress reporting
    - Identifier extraction
    - Reference parsing
    """
    
    # Subclasses should override these
    RESOURCE_TYPE = "Base"
    NODE_LABEL = "Base"
    
    def __init__(self, *, neo4j_uri: str, neo4j_user: str, neo4j_password: str, import_tag: Optional[str] = None):
        """
        Initialize the importer.
        
        Args:
            neo4j_uri: Neo4j connection URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            import_tag: Optional tag to identify this import run (only set on new nodes)
        """
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.import_tag = import_tag
    
    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()
    
    @staticmethod
    def _extract_identifiers(*, resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract identifier information from a FHIR resource.
        
        Args:
            resource: The FHIR resource dictionary
        
        Returns:
            Dictionary with identifier_systems list, identifier_values list, and npi if present
        """
        result = {
            'identifier_systems': [],
            'identifier_values': [],
            'npi': None
        }
        
        identifiers = resource.get('identifier', [])
        if not isinstance(identifiers, list):
            identifiers = [identifiers]
        
        for identifier in identifiers:
            if not isinstance(identifier, dict):
                continue
            
            system = identifier.get('system', '')
            value = identifier.get('value', '')
            
            if system:
                result['identifier_systems'].append(system)
            if value:
                result['identifier_values'].append(value)
            
            # Check for NPI
            if 'npi' in system.lower() or system == 'http://hl7.org/fhir/sid/us-npi':
                result['npi'] = value
        
        return result
    
    @staticmethod
    def _parse_reference(*, reference: Optional[str]) -> Optional[str]:
        """
        Extract the resource ID from a FHIR reference.
        
        Args:
            reference: FHIR reference string (e.g., "Practitioner/12345")
        
        Returns:
            The resource ID or None
        """
        if not reference:
            return None
        
        # Handle both "ResourceType/id" and just "id" formats
        if '/' in reference:
            parts = reference.split('/')
            return parts[-1]
        
        return reference
    
    @staticmethod
    def _safe_get(*, resource: Dict[str, Any], path: str, default: Any = None) -> Any:
        """
        Safely get a nested value from a dictionary using dot notation.
        
        Args:
            resource: The dictionary to search
            path: Dot-separated path (e.g., "name.0.family")
            default: Default value if not found
        
        Returns:
            The value or default
        """
        keys = path.split('.')
        value = resource
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list) and key.isdigit():
                idx = int(key)
                value = value[idx] if idx < len(value) else None
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    def _log(self, *, message: str) -> None:
        """
        Log a progress message.
        
        Args:
            message: The message to log
        """
        print(message, file=sys.stderr)
    
    def read_ndjson(self, *, filepath: Path) -> List[Dict[str, Any]]:
        """
        Read and parse an NDJSON file.
        
        Args:
            filepath: Path to the NDJSON file
        
        Returns:
            List of parsed JSON objects
        """
        records = []
        errors = 0
        
        self._log(message=f"Reading {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    
                    # Verify it's the expected resource type
                    resource_type = record.get('resourceType')
                    if resource_type != self.RESOURCE_TYPE:
                        self._log(message=f"Warning: Line {line_num} has resourceType '{resource_type}', expected '{self.RESOURCE_TYPE}'")
                        continue
                    
                    records.append(record)
                    
                except json.JSONDecodeError as e:
                    errors += 1
                    self._log(message=f"Error parsing line {line_num}: {e}")
                    continue
        
        self._log(message=f"Read {len(records)} valid {self.RESOURCE_TYPE} resources ({errors} errors)")
        
        return records
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of resources into Neo4j.
        
        This method should be overridden by subclasses to implement
        resource-specific import logic.
        
        Args:
            session: Neo4j session
            batch: List of FHIR resources to import
        
        Returns:
            Number of nodes created/updated
        """
        raise NotImplementedError("Subclasses must implement import_batch")
    
    def import_file(self, *, filepath: Path, batch_size: int = 1000) -> None:
        """
        Import an entire NDJSON file into Neo4j.
        
        Args:
            filepath: Path to the NDJSON file
            batch_size: Number of records to process per batch
        """
        records = self.read_ndjson(filepath=filepath)
        
        if not records:
            print(f"No {self.RESOURCE_TYPE} records to import from {filepath}")
            return
        
        print(f"Importing {len(records)} {self.RESOURCE_TYPE} resources...")
        
        # Track timing for this resource type
        start_time = time.time()
        
        with self.driver.session() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                count = self.import_batch(session=session, batch=batch)
                
                # Calculate timing information
                records_processed = i + len(batch)
                elapsed_time = time.time() - start_time
                time_per_record = elapsed_time / records_processed if records_processed > 0 else 0
                
                # Format time displays
                elapsed_str = self._format_time(seconds=elapsed_time)
                time_per_record_str = f"{time_per_record:.4f}s" if time_per_record >= 0.0001 else f"{time_per_record*1000:.2f}ms"
                
                print(f"  Processed {records_processed}/{len(records)} ({count} nodes created/updated) | "
                      f"Time so far: {elapsed_str} | Time per record: {time_per_record_str}")
        
        # Print summary when resource type import is complete
        total_time = time.time() - start_time
        total_time_str = self._format_time(seconds=total_time)
        avg_time_per_record = total_time / len(records) if len(records) > 0 else 0
        avg_time_str = f"{avg_time_per_record:.4f}s" if avg_time_per_record >= 0.0001 else f"{avg_time_per_record*1000:.2f}ms"
        
        print(f"\n{'='*60}")
        print(f"Completed import of {self.RESOURCE_TYPE}")
        print(f"Total records: {len(records)}")
        print(f"Total time: {total_time_str}")
        print(f"Average time per record: {avg_time_str}")
        print(f"{'='*60}\n")
    
    @staticmethod
    def _format_time(*, seconds: float) -> str:
        """
        Format time in seconds to a human-readable string.
        
        Args:
            seconds: Time in seconds
        
        Returns:
            Formatted time string (e.g., "1h 23m 45s" or "2m 30s" or "45s")
        """
        if seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = seconds % 60
            return f"{minutes}m {secs:.2f}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = seconds % 60
            return f"{hours}h {minutes}m {secs:.2f}s"
