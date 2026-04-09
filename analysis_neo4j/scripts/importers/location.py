"""
Location resource importer.
"""

from typing import Dict, Any, List, Optional
from neo4j import Session
from .base import BaseImporter, _to_json_string


class LocationImporter(BaseImporter):
    """Import Location resources into Neo4j."""
    
    RESOURCE_TYPE = "Location"
    NODE_LABEL = "Location"
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of Location resources.
        
        Args:
            session: Neo4j session
            batch: List of Location FHIR resources
        
        Returns:
            Number of nodes created/updated
        """
        # Prepare data for batch import
        location_data = []
        
        for resource in batch:
            fhir_id = resource.get('id')
            if not fhir_id:
                self._log(message=f"Skipping Location without id: {resource}")
                continue
            
            # Extract identifiers as objects
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract status
            status = resource.get('status')
            
            # Extract name
            name = resource.get('name')
            
            # Extract type
            location_types = []
            for loc_type in resource.get('type', []):
                if isinstance(loc_type, dict):
                    coding = loc_type.get('coding', [])
                    if coding and isinstance(coding, list):
                        for code in coding:
                            if isinstance(code, dict):
                                location_types.append(code.get('display', code.get('code', '')))
            
            # Extract address (Location has single address, not array)
            address = None
            address_raw = resource.get('address', {})
            if isinstance(address_raw, dict) and address_raw:
                # Join line array into single string
                lines = address_raw.get('line', [])
                line_str = ', '.join(lines) if isinstance(lines, list) else str(lines) if lines else None
                
                address = {}
                if line_str:
                    address['line'] = line_str
                if address_raw.get('city'):
                    address['city'] = address_raw.get('city')
                if address_raw.get('state'):
                    address['state'] = address_raw.get('state')
                if address_raw.get('postalCode'):
                    address['postalCode'] = address_raw.get('postalCode')
                if address_raw.get('country'):
                    address['country'] = address_raw.get('country')
                if address_raw.get('type'):
                    address['type'] = address_raw.get('type')
                if address_raw.get('use'):
                    address['use'] = address_raw.get('use')
                
                # If address is empty after all checks, set to None
                if not address:
                    address = None
            
            # Extract position (lat/long)
            position = resource.get('position', {})
            latitude = None
            longitude = None
            if isinstance(position, dict):
                latitude = position.get('latitude')
                longitude = position.get('longitude')
            
            # Extract managing organization
            managing_org = resource.get('managingOrganization', {})
            managing_org_reference = managing_org.get('reference') if isinstance(managing_org, dict) else None
            managing_org_id = self._parse_reference(reference=managing_org_reference)
            
            # Extract part of (parent location)
            part_of = resource.get('partOf', {})
            part_of_reference = part_of.get('reference') if isinstance(part_of, dict) else None
            part_of_id = self._parse_reference(reference=part_of_reference)
            
            # Extract endpoint references
            endpoint_refs = resource.get('endpoint', [])
            endpoint_ids = []
            if isinstance(endpoint_refs, list):
                for ep_ref in endpoint_refs:
                    if isinstance(ep_ref, dict):
                        ref = ep_ref.get('reference')
                        ep_id = self._parse_reference(reference=ref)
                        if ep_id:
                            endpoint_ids.append(ep_id)
            
            location_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'status': status,
                'name': name,
                'location_types': location_types,
                'address': _to_json_string(obj=address),
                'latitude': latitude,
                'longitude': longitude,
                'managing_organization_id': managing_org_id,
                'part_of_id': part_of_id,
                'endpoint_ids': endpoint_ids,
                'identifiers': _to_json_string(obj=identifiers),
                'import_tag': self.import_tag,
            })
        
        # Batch import nodes - use CREATE or MERGE based on mode
        if self.use_create:
            query = """
            UNWIND $batch AS loc
            CREATE (l:Location {
                fhir_id: loc.fhir_id,
                import_tag: loc.import_tag,
                resource_type: loc.resource_type,
                status: loc.status,
                name: loc.name,
                location_types: loc.location_types,
                address: loc.address,
                latitude: loc.latitude,
                longitude: loc.longitude,
                managing_organization_reference: loc.managing_organization_id,
                part_of_reference: loc.part_of_id,
                endpoint_references: loc.endpoint_ids,
                identifiers: loc.identifiers
            })
            RETURN count(l) AS count
            """
        else:
            query = """
            UNWIND $batch AS loc
            MERGE (l:Location {fhir_id: loc.fhir_id})
            ON CREATE SET l.import_tag = loc.import_tag
            SET l.resource_type = loc.resource_type,
                l.status = loc.status,
                l.name = loc.name,
                l.location_types = loc.location_types,
                l.address = loc.address,
                l.latitude = loc.latitude,
                l.longitude = loc.longitude,
                l.managing_organization_reference = loc.managing_organization_id,
                l.part_of_reference = loc.part_of_id,
                l.endpoint_references = loc.endpoint_ids,
                l.identifiers = loc.identifiers
            RETURN count(l) AS count
            """
        
        result = session.run(query, batch=location_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, location_data=location_data, use_create=self.use_create)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, location_data: List[Dict[str, Any]], use_create: bool = False) -> None:
        """
        Create relationships between Location and other resources.
        
        Args:
            session: Neo4j session
            location_data: List of processed location data
            use_create: If True, use CREATE instead of MERGE (faster but not idempotent)
        """
        # Choose operation based on mode
        rel_op = "CREATE" if use_create else "MERGE"
        
        # Create managing organization relationships
        org_query = f"""
        UNWIND $batch AS loc
        MATCH (l:Location {{fhir_id: loc.fhir_id}})
        MATCH (o:Organization {{fhir_id: loc.managing_organization_id}})
        {rel_op} (o)-[:MANAGES]->(l)
        """
        session.run(org_query, batch=[l for l in location_data if l.get('managing_organization_id')])
        
        # Create parent location relationships
        parent_query = f"""
        UNWIND $batch AS loc
        MATCH (child:Location {{fhir_id: loc.fhir_id}})
        MATCH (parent:Location {{fhir_id: loc.part_of_id}})
        {rel_op} (child)-[:PART_OF]->(parent)
        """
        session.run(parent_query, batch=[l for l in location_data if l.get('part_of_id')])
        
        # Create Endpoint relationships
        ep_query = f"""
        UNWIND $batch AS loc
        MATCH (l:Location {{fhir_id: loc.fhir_id}})
        UNWIND loc.endpoint_ids AS ep_id
        MATCH (e:Endpoint {{fhir_id: ep_id}})
        {rel_op} (l)-[:HAS_ENDPOINT]->(e)
        """
        session.run(ep_query, batch=[l for l in location_data if l.get('endpoint_ids')])
