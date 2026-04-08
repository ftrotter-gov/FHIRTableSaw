"""
Base importer class with common functionality for all FHIR resource importers.
"""

import json
import sys
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from neo4j import GraphDatabase, Session


def _to_json_string(*, obj: Any) -> Optional[str]:
    """
    Convert a Python object to JSON string for Neo4j storage.
    
    Neo4j cannot store nested maps/objects, only primitives and arrays of primitives.
    This helper converts complex objects to JSON strings.
    
    Args:
        obj: The object to convert
    
    Returns:
        JSON string or None if obj is None or empty
    """
    if obj is None or (isinstance(obj, (list, dict)) and not obj):
        return None
    return json.dumps(obj)


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
    
    def __init__(self, *, neo4j_uri: str, neo4j_user: str, neo4j_password: str, import_tag: Optional[str] = None, use_create: bool = False):
        """
        Initialize the importer.
        
        Args:
            neo4j_uri: Neo4j connection URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            import_tag: Optional tag to identify this import run (only set on new nodes)
            use_create: If True, use CREATE instead of MERGE (much faster, but not idempotent)
        """
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.import_tag = import_tag
        self.use_create = use_create
    
    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()
    
    @staticmethod
    def _extract_identifiers(*, resource: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Extract identifier information from a FHIR resource as pairs.
        
        Identifiers are meaningless without their system, so they are kept together.
        
        Args:
            resource: The FHIR resource dictionary
        
        Returns:
            List of identifier objects with {system, value} pairs
        """
        identifiers = []
        
        identifier_list = resource.get('identifier', [])
        if not isinstance(identifier_list, list):
            identifier_list = [identifier_list]
        
        for identifier in identifier_list:
            if not isinstance(identifier, dict):
                continue
            
            system = identifier.get('system', '')
            value = identifier.get('value', '')
            
            if system and value:
                identifiers.append({
                    'system': system,
                    'value': value
                })
        
        return identifiers
    
    @staticmethod
    def _extract_npi_single(*, identifiers: List[Dict[str, str]]) -> Optional[str]:
        """
        Extract single NPI from identifier list (for Practitioners).
        
        Args:
            identifiers: List of identifier objects
        
        Returns:
            NPI value or None
        """
        for identifier in identifiers:
            system = identifier.get('system', '')
            if 'npi' in system.lower() or system == 'http://hl7.org/fhir/sid/us-npi':
                return identifier.get('value')
        return None
    
    @staticmethod
    def _extract_npi_list(*, identifiers: List[Dict[str, str]]) -> List[str]:
        """
        Extract all NPIs from identifier list (for Organizations).
        
        Organizations can have multiple NPIs.
        
        Args:
            identifiers: List of identifier objects
        
        Returns:
            List of NPI values
        """
        npis = []
        for identifier in identifiers:
            system = identifier.get('system', '')
            if 'npi' in system.lower() or system == 'http://hl7.org/fhir/sid/us-npi':
                value = identifier.get('value')
                if value:
                    npis.append(value)
        return npis
    
    @staticmethod
    def _extract_addresses(*, resource: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract addresses as coherent objects.
        
        Each address includes line, city, state, postalCode, country, type, and use.
        
        Args:
            resource: The FHIR resource dictionary
        
        Returns:
            List of address objects
        """
        addresses = []
        
        address_list = resource.get('address', [])
        if not isinstance(address_list, list):
            address_list = [address_list] if address_list else []
        
        for addr in address_list:
            if not isinstance(addr, dict):
                continue
            
            # Join line array into single string
            lines = addr.get('line', [])
            line_str = ', '.join(lines) if isinstance(lines, list) else str(lines) if lines else None
            
            address_obj = {}
            if line_str:
                address_obj['line'] = line_str
            if addr.get('city'):
                address_obj['city'] = addr.get('city')
            if addr.get('state'):
                address_obj['state'] = addr.get('state')
            if addr.get('postalCode'):
                address_obj['postalCode'] = addr.get('postalCode')
            if addr.get('country'):
                address_obj['country'] = addr.get('country')
            if addr.get('type'):
                address_obj['type'] = addr.get('type')
            if addr.get('use'):
                address_obj['use'] = addr.get('use')
            
            if address_obj:  # Only add if not empty
                addresses.append(address_obj)
        
        return addresses
    
    @staticmethod
    def _extract_telecoms(*, resource: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Extract telecom data separated by type.
        
        Args:
            resource: The FHIR resource dictionary
        
        Returns:
            Dictionary with emails, phones, and faxes lists
        """
        result = {
            'emails': [],
            'phones': [],
            'faxes': []
        }
        
        telecom_list = resource.get('telecom', [])
        if not isinstance(telecom_list, list):
            telecom_list = [telecom_list] if telecom_list else []
        
        for telecom in telecom_list:
            if not isinstance(telecom, dict):
                continue
            
            system = telecom.get('system', '').lower()
            value = telecom.get('value')
            
            if not value:
                continue
            
            if system == 'email':
                result['emails'].append(value)
            elif system == 'phone':
                result['phones'].append(value)
            elif system == 'fax':
                result['faxes'].append(value)
        
        return result
    
    @staticmethod
    def _is_email(*, address: str) -> bool:
        """
        Check if an address string is an email address.
        
        Args:
            address: The address string to check
        
        Returns:
            True if address appears to be an email
        """
        import re
        if not address:
            return False
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        return bool(email_pattern.match(address.strip()))
    
    @staticmethod
    def _extract_state_from_org_reference(*, reference: Optional[str]) -> Optional[str]:
        """
        Extract state code from an organization reference.
        
        Example: "Organization/Organization-State-WY" -> "WY"
        
        Args:
            reference: FHIR reference string
        
        Returns:
            State code or None
        """
        if not reference:
            return None
        
        # Pattern: Organization/Organization-State-XX or similar
        parts = reference.split('-')
        if len(parts) >= 2:
            # Last part is usually the state code
            state_code = parts[-1]
            # Verify it looks like a state code (2 uppercase letters)
            if len(state_code) == 2 and state_code.isupper():
                return state_code
        
        return None
    
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
    
    def import_file(self, *, filepath: Path, batch_size: int = 1000, limit: Optional[int] = None) -> None:
        """
        Import an entire NDJSON file into Neo4j.
        
        Args:
            filepath: Path to the NDJSON file
            batch_size: Number of records to process per batch
            limit: Optional limit on number of records to import (useful for testing)
        """
        records = self.read_ndjson(filepath=filepath)
        
        if not records:
            print(f"No {self.RESOURCE_TYPE} records to import from {filepath}")
            return
        
        # Apply limit if specified
        if limit is not None and limit > 0:
            original_count = len(records)
            records = records[:limit]
            print(f"Limiting import to {len(records)} of {original_count} {self.RESOURCE_TYPE} resources (--limit {limit})...")
        else:
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
