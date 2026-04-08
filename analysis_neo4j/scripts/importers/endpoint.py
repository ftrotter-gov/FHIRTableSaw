"""
Endpoint resource importer.
"""

from typing import Dict, Any, List, Optional
from neo4j import Session
from .base import BaseImporter, _to_json_string


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
            
            # Extract identifiers as objects
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract status
            status = resource.get('status')
            
            # Extract address and categorize as FHIR or Direct
            address = resource.get('address')
            fhir_address = None
            direct_address = None
            
            if address:
                if self._is_email(address=address):
                    direct_address = address
                else:
                    fhir_address = address
            
            # Extract rank from extensions
            rank = self._extract_rank(resource=resource)
            
            # Extract managing organization
            managing_org = resource.get('managingOrganization', {})
            managing_org_reference = managing_org.get('reference') if isinstance(managing_org, dict) else None
            managing_org_id = self._parse_reference(reference=managing_org_reference)
            
            endpoint_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'status': status,
                'FHIR_address': fhir_address,
                'Direct_address': direct_address,
                'rank': rank,
                'identifiers': _to_json_string(obj=identifiers),
                'managing_organization_id': managing_org_id,
                'import_tag': self.import_tag,
            })
        
        # Batch import nodes - use CREATE or MERGE based on mode
        if self.use_create:
            query = """
            UNWIND $batch AS ep
            CREATE (e:Endpoint {
                fhir_id: ep.fhir_id,
                import_tag: ep.import_tag,
                resource_type: ep.resource_type,
                status: ep.status,
                FHIR_address: ep.FHIR_address,
                Direct_address: ep.Direct_address,
                rank: ep.rank,
                identifiers: ep.identifiers,
                managing_organization_reference: ep.managing_organization_id
            })
            RETURN count(e) AS count
            """
        else:
            query = """
            UNWIND $batch AS ep
            MERGE (e:Endpoint {fhir_id: ep.fhir_id})
            ON CREATE SET e.import_tag = ep.import_tag
            SET e.resource_type = ep.resource_type,
                e.status = ep.status,
                e.FHIR_address = ep.FHIR_address,
                e.Direct_address = ep.Direct_address,
                e.rank = ep.rank,
                e.identifiers = ep.identifiers,
                e.managing_organization_reference = ep.managing_organization_id
            RETURN count(e) AS count
            """
        
        result = session.run(query, batch=endpoint_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, endpoint_data=endpoint_data)
        
        return node_count
    
    @staticmethod
    def _extract_rank(*, resource: Dict[str, Any]) -> Optional[int]:
        """
        Extract rank from endpoint extensions.
        
        Args:
            resource: The FHIR Endpoint resource
        
        Returns:
            Rank value or None
        """
        extensions = resource.get('extension', [])
        if not isinstance(extensions, list):
            extensions = [extensions] if extensions else []
        
        for ext in extensions:
            if not isinstance(ext, dict):
                continue
            
            url = ext.get('url', '')
            if 'base-ext-endpoint-rank' in url:
                rank = ext.get('valuePositiveInt')
                if rank is not None:
                    return rank
        
        return None
    
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
