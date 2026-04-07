"""
Endpoint resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter


class EndpointImporter(BaseImporter):
    """Import Endpoint resources into Neo4j."""
    
    RESOURCE_TYPE = "Endpoint"
    NODE_LABEL = "Endpoint"
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of Endpoint resources.
        
        Args:
            session: Neo4j session
            batch: List of Endpoint FHIR resources
        
        Returns:
            Number of nodes created/updated
        """
        # Prepare data for batch import
        endpoint_data = []
        
        for resource in batch:
            fhir_id = resource.get('id')
            if not fhir_id:
                self._log(message=f"Skipping Endpoint without id: {resource}")
                continue
            
            # Extract identifiers
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract status
            status = resource.get('status')
            
            # Extract connection type
            connection_type = None
            conn_type_obj = resource.get('connectionType', {})
            if isinstance(conn_type_obj, dict):
                coding = conn_type_obj.get('coding', [])
                if coding and isinstance(coding, list) and len(coding) > 0:
                    connection_type = coding[0].get('display', coding[0].get('code', ''))
            
            # Extract name
            name = resource.get('name')
            
            # Extract address (URL)
            address = resource.get('address')
            
            # Extract payload types
            payload_types = []
            for payload in resource.get('payloadType', []):
                if isinstance(payload, dict):
                    coding = payload.get('coding', [])
                    if coding and isinstance(coding, list):
                        for code in coding:
                            if isinstance(code, dict):
                                payload_types.append(code.get('display', code.get('code', '')))
            
            # Extract managing organization
            managing_org = resource.get('managingOrganization', {})
            managing_org_reference = managing_org.get('reference') if isinstance(managing_org, dict) else None
            managing_org_id = self._parse_reference(reference=managing_org_reference)
            
            endpoint_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'status': status,
                'connection_type': connection_type,
                'name': name,
                'address': address,
                'payload_types': payload_types,
                'managing_organization_id': managing_org_id,
                'identifier_systems': identifiers['identifier_systems'],
                'identifier_values': identifiers['identifier_values'],
            })
        
        # Batch import nodes
        query = """
        UNWIND $batch AS ep
        MERGE (e:Endpoint {fhir_id: ep.fhir_id})
        SET e.resource_type = ep.resource_type,
            e.status = ep.status,
            e.connection_type = ep.connection_type,
            e.name = ep.name,
            e.address = ep.address,
            e.payload_types = ep.payload_types,
            e.managing_organization_reference = ep.managing_organization_id,
            e.identifier_systems = ep.identifier_systems,
            e.identifier_values = ep.identifier_values
        RETURN count(e) AS count
        """
        
        result = session.run(query, batch=endpoint_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, endpoint_data=endpoint_data)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, endpoint_data: List[Dict[str, Any]]) -> None:
        """
        Create relationships between Endpoint and other resources.
        
        Args:
            session: Neo4j session
            endpoint_data: List of processed endpoint data
        """
        # Create managing organization relationships
        org_query = """
        UNWIND $batch AS ep
        MATCH (e:Endpoint {fhir_id: ep.fhir_id})
        MATCH (o:Organization {fhir_id: ep.managing_organization_id})
        MERGE (o)-[:MANAGES]->(e)
        """
        session.run(org_query, batch=[e for e in endpoint_data if e.get('managing_organization_id')])
