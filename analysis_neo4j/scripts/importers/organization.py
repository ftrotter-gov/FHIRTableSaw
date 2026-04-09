"""
Organization resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter, _to_json_string


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
            
            # Extract identifiers as objects
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract NPIs as array (organizations can have multiple)
            npis = self._extract_npi_list(identifiers=identifiers)
            
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
            
            # Extract addresses as coherent objects
            addresses = self._extract_addresses(resource=resource)
            
            # Extract telecoms separated by type
            telecoms = self._extract_telecoms(resource=resource)
            
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
                'npis': npis,
                'identifiers': _to_json_string(obj=identifiers),
                'addresses': _to_json_string(obj=addresses),
                'emails': telecoms['emails'],
                'phones': telecoms['phones'],
                'faxes': telecoms['faxes'],
                'part_of_id': part_of_id,
                'endpoint_ids': endpoint_ids,
                'import_tag': self.import_tag,
            })
        
        # Batch import nodes - use CREATE or MERGE based on mode
        if self.use_create:
            query = """
            UNWIND $batch AS org
            CREATE (o:Organization {
                fhir_id: org.fhir_id,
                import_tag: org.import_tag,
                resource_type: org.resource_type,
                name: org.name,
                active: org.active,
                org_types: org.org_types,
                npis: org.npis,
                identifiers: org.identifiers,
                addresses: org.addresses,
                emails: org.emails,
                phones: org.phones,
                faxes: org.faxes,
                part_of_reference: org.part_of_id,
                endpoint_references: org.endpoint_ids
            })
            RETURN count(o) AS count
            """
        else:
            query = """
            UNWIND $batch AS org
            MERGE (o:Organization {fhir_id: org.fhir_id})
            ON CREATE SET o.import_tag = org.import_tag
            SET o.resource_type = org.resource_type,
                o.name = org.name,
                o.active = org.active,
                o.org_types = org.org_types,
                o.npis = org.npis,
                o.identifiers = org.identifiers,
                o.addresses = org.addresses,
                o.emails = org.emails,
                o.phones = org.phones,
                o.faxes = org.faxes,
                o.part_of_reference = org.part_of_id,
                o.endpoint_references = org.endpoint_ids
            RETURN count(o) AS count
            """
        
        result = session.run(query, batch=org_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, org_data=org_data, use_create=self.use_create)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, org_data: List[Dict[str, Any]], use_create: bool = False) -> None:
        """
        Create relationships between Organization and other resources.
        
        Args:
            session: Neo4j session
            org_data: List of processed organization data
            use_create: If True, use CREATE instead of MERGE (faster but not idempotent)
        """
        # Choose operation based on mode
        rel_op = "CREATE" if use_create else "MERGE"
        
        # Create parent organization relationships
        parent_query = f"""
        UNWIND $batch AS org
        MATCH (child:Organization {{fhir_id: org.fhir_id}})
        MATCH (parent:Organization {{fhir_id: org.part_of_id}})
        {rel_op} (child)-[:PART_OF]->(parent)
        """
        session.run(parent_query, batch=[o for o in org_data if o.get('part_of_id')])
        
        # Create Endpoint relationships
        ep_query = f"""
        UNWIND $batch AS org
        MATCH (o:Organization {{fhir_id: org.fhir_id}})
        UNWIND org.endpoint_ids AS ep_id
        MATCH (e:Endpoint {{fhir_id: ep_id}})
        {rel_op} (o)-[:HAS_ENDPOINT]->(e)
        """
        session.run(ep_query, batch=[o for o in org_data if o.get('endpoint_ids')])
