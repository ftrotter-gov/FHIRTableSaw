"""
Location resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter


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
            
            # Extract identifiers
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
            
            # Extract address
            address = resource.get('address', {})
            address_line = None
            city = None
            state = None
            postal_code = None
            country = None
            
            if isinstance(address, dict):
                lines = address.get('line', [])
                if isinstance(lines, list) and lines:
                    address_line = ', '.join(lines)
                city = address.get('city')
                state = address.get('state')
                postal_code = address.get('postalCode')
                country = address.get('country')
            
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
                'address_line': address_line,
                'city': city,
                'state': state,
                'postal_code': postal_code,
                'country': country,
                'latitude': latitude,
                'longitude': longitude,
                'managing_organization_id': managing_org_id,
                'part_of_id': part_of_id,
                'endpoint_ids': endpoint_ids,
                'identifier_systems': identifiers['identifier_systems'],
                'identifier_values': identifiers['identifier_values'],
                'import_tag': self.import_tag,
            })
        
        # Batch import nodes
        query = """
        UNWIND $batch AS loc
        MERGE (l:Location {fhir_id: loc.fhir_id})
        ON CREATE SET l.import_tag = loc.import_tag
        SET l.resource_type = loc.resource_type,
            l.status = loc.status,
            l.name = loc.name,
            l.location_types = loc.location_types,
            l.address_line = loc.address_line,
            l.city = loc.city,
            l.state = loc.state,
            l.postal_code = loc.postal_code,
            l.country = loc.country,
            l.latitude = loc.latitude,
            l.longitude = loc.longitude,
            l.managing_organization_reference = loc.managing_organization_id,
            l.part_of_reference = loc.part_of_id,
            l.endpoint_references = loc.endpoint_ids,
            l.identifier_systems = loc.identifier_systems,
            l.identifier_values = loc.identifier_values
        RETURN count(l) AS count
        """
        
        result = session.run(query, batch=location_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, location_data=location_data)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, location_data: List[Dict[str, Any]]) -> None:
        """
        Create relationships between Location and other resources.
        
        Args:
            session: Neo4j session
            location_data: List of processed location data
        """
        # Create managing organization relationships
        org_query = """
        UNWIND $batch AS loc
        MATCH (l:Location {fhir_id: loc.fhir_id})
        MATCH (o:Organization {fhir_id: loc.managing_organization_id})
        MERGE (o)-[:MANAGES]->(l)
        """
        session.run(org_query, batch=[l for l in location_data if l.get('managing_organization_id')])
        
        # Create parent location relationships
        parent_query = """
        UNWIND $batch AS loc
        MATCH (child:Location {fhir_id: loc.fhir_id})
        MATCH (parent:Location {fhir_id: loc.part_of_id})
        MERGE (child)-[:PART_OF]->(parent)
        """
        session.run(parent_query, batch=[l for l in location_data if l.get('part_of_id')])
        
        # Create Endpoint relationships
        ep_query = """
        UNWIND $batch AS loc
        MATCH (l:Location {fhir_id: loc.fhir_id})
        UNWIND loc.endpoint_ids AS ep_id
        MATCH (e:Endpoint {fhir_id: ep_id})
        MERGE (l)-[:HAS_ENDPOINT]->(e)
        """
        session.run(ep_query, batch=[l for l in location_data if l.get('endpoint_ids')])
