"""
Organization resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter


class OrganizationImporter(BaseImporter):
    """Import Organization resources into Neo4j."""
    
    RESOURCE_TYPE = "Organization"
    NODE_LABEL = "Organization"
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of Organization resources.
        
        Args:
            session: Neo4j session
            batch: List of Organization FHIR resources
        
        Returns:
            Number of nodes created/updated
        """
        # Prepare data for batch import
        org_data = []
        
        for resource in batch:
            fhir_id = resource.get('id')
            if not fhir_id:
                self._log(message=f"Skipping Organization without id: {resource}")
                continue
            
            # Extract identifiers including NPI
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract name
            name = resource.get('name')
            
            # Extract active status
            active = resource.get('active')
            
            # Extract type
            org_types = []
            for org_type in resource.get('type', []):
                if isinstance(org_type, dict):
                    coding = org_type.get('coding', [])
                    if coding and isinstance(coding, list):
                        for code in coding:
                            if isinstance(code, dict):
                                org_types.append(code.get('display', code.get('code', '')))
            
            # Extract address
            addresses = resource.get('address', [])
            address_lines = []
            cities = []
            states = []
            postal_codes = []
            if isinstance(addresses, list):
                for addr in addresses:
                    if isinstance(addr, dict):
                        city = addr.get('city')
                        state = addr.get('state')
                        postal = addr.get('postalCode')
                        if city:
                            cities.append(city)
                        if state:
                            states.append(state)
                        if postal:
                            postal_codes.append(postal)
                        
                        lines = addr.get('line', [])
                        if isinstance(lines, list):
                            address_lines.extend(lines)
            
            # Extract telecom
            telecoms = []
            for telecom in resource.get('telecom', []):
                if isinstance(telecom, dict):
                    system = telecom.get('system')
                    value = telecom.get('value')
                    if system and value:
                        telecoms.append(f"{system}:{value}")
            
            # Extract partOf reference (parent organization)
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
            
            org_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'name': name,
                'active': active,
                'org_types': org_types,
                'address_lines': address_lines,
                'cities': cities,
                'states': states,
                'postal_codes': postal_codes,
                'telecoms': telecoms,
                'part_of_id': part_of_id,
                'endpoint_ids': endpoint_ids,
                'npi': identifiers['npi'],
                'identifier_systems': identifiers['identifier_systems'],
                'identifier_values': identifiers['identifier_values'],
            })
        
        # Batch import nodes
        query = """
        UNWIND $batch AS org
        MERGE (o:Organization {fhir_id: org.fhir_id})
        SET o.resource_type = org.resource_type,
            o.name = org.name,
            o.active = org.active,
            o.org_types = org.org_types,
            o.address_lines = org.address_lines,
            o.cities = org.cities,
            o.states = org.states,
            o.postal_codes = org.postal_codes,
            o.telecoms = org.telecoms,
            o.part_of_reference = org.part_of_id,
            o.endpoint_references = org.endpoint_ids,
            o.npi = org.npi,
            o.identifier_systems = org.identifier_systems,
            o.identifier_values = org.identifier_values
        RETURN count(o) AS count
        """
        
        result = session.run(query, batch=org_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, org_data=org_data)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, org_data: List[Dict[str, Any]]) -> None:
        """
        Create relationships between Organization and other resources.
        
        Args:
            session: Neo4j session
            org_data: List of processed organization data
        """
        # Create parent organization relationships
        parent_query = """
        UNWIND $batch AS org
        MATCH (child:Organization {fhir_id: org.fhir_id})
        MATCH (parent:Organization {fhir_id: org.part_of_id})
        MERGE (child)-[:PART_OF]->(parent)
        """
        session.run(parent_query, batch=[o for o in org_data if o.get('part_of_id')])
        
        # Create Endpoint relationships
        ep_query = """
        UNWIND $batch AS org
        MATCH (o:Organization {fhir_id: org.fhir_id})
        UNWIND org.endpoint_ids AS ep_id
        MATCH (e:Endpoint {fhir_id: ep_id})
        MERGE (o)-[:HAS_ENDPOINT]->(e)
        """
        session.run(ep_query, batch=[o for o in org_data if o.get('endpoint_ids')])
