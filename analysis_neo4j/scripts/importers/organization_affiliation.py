"""
OrganizationAffiliation resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter


class OrganizationAffiliationImporter(BaseImporter):
    """Import OrganizationAffiliation resources into Neo4j."""
    
    RESOURCE_TYPE = "OrganizationAffiliation"
    NODE_LABEL = "OrganizationAffiliation"
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of OrganizationAffiliation resources.
        
        Args:
            session: Neo4j session
            batch: List of OrganizationAffiliation FHIR resources
        
        Returns:
            Number of nodes created/updated
        """
        # Prepare data for batch import
        affiliation_data = []
        
        for resource in batch:
            fhir_id = resource.get('id')
            if not fhir_id:
                self._log(message=f"Skipping OrganizationAffiliation without id: {resource}")
                continue
            
            # Extract identifiers
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract active status
            active = resource.get('active')
            
            # Extract organization reference (the primary organization)
            org_ref = resource.get('organization', {})
            org_reference = org_ref.get('reference') if isinstance(org_ref, dict) else None
            org_id = self._parse_reference(reference=org_reference)
            
            # Extract participating organization reference
            participating_org_ref = resource.get('participatingOrganization', {})
            participating_org_reference = participating_org_ref.get('reference') if isinstance(participating_org_ref, dict) else None
            participating_org_id = self._parse_reference(reference=participating_org_reference)
            
            # Extract code (type of affiliation)
            codes = []
            for code in resource.get('code', []):
                if isinstance(code, dict):
                    coding = code.get('coding', [])
                    if coding and isinstance(coding, list):
                        for c in coding:
                            if isinstance(c, dict):
                                codes.append(c.get('display', c.get('code', '')))
            
            # Extract specialty
            specialties = []
            for specialty in resource.get('specialty', []):
                if isinstance(specialty, dict):
                    coding = specialty.get('coding', [])
                    if coding and isinstance(coding, list):
                        for c in coding:
                            if isinstance(c, dict):
                                specialties.append(c.get('display', c.get('code', '')))
            
            # Extract location references
            location_refs = resource.get('location', [])
            location_ids = []
            if isinstance(location_refs, list):
                for loc_ref in location_refs:
                    if isinstance(loc_ref, dict):
                        ref = loc_ref.get('reference')
                        loc_id = self._parse_reference(reference=ref)
                        if loc_id:
                            location_ids.append(loc_id)
            
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
            
            affiliation_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'active': active,
                'organization_id': org_id,
                'participating_organization_id': participating_org_id,
                'codes': codes,
                'specialties': specialties,
                'location_ids': location_ids,
                'endpoint_ids': endpoint_ids,
                'identifier_systems': identifiers['identifier_systems'],
                'identifier_values': identifiers['identifier_values'],
            })
        
        # Batch import nodes
        query = """
        UNWIND $batch AS aff
        MERGE (oa:OrganizationAffiliation {fhir_id: aff.fhir_id})
        SET oa.resource_type = aff.resource_type,
            oa.active = aff.active,
            oa.organization_reference = aff.organization_id,
            oa.participating_organization_reference = aff.participating_organization_id,
            oa.codes = aff.codes,
            oa.specialties = aff.specialties,
            oa.location_references = aff.location_ids,
            oa.endpoint_references = aff.endpoint_ids,
            oa.identifier_systems = aff.identifier_systems,
            oa.identifier_values = aff.identifier_values
        RETURN count(oa) AS count
        """
        
        result = session.run(query, batch=affiliation_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, affiliation_data=affiliation_data)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, affiliation_data: List[Dict[str, Any]]) -> None:
        """
        Create relationships between OrganizationAffiliation and other resources.
        
        Args:
            session: Neo4j session
            affiliation_data: List of processed affiliation data
        """
        # Create primary organization relationships
        org_query = """
        UNWIND $batch AS aff
        MATCH (oa:OrganizationAffiliation {fhir_id: aff.fhir_id})
        MATCH (o:Organization {fhir_id: aff.organization_id})
        MERGE (o)-[:HAS_AFFILIATION]->(oa)
        """
        session.run(org_query, batch=[a for a in affiliation_data if a.get('organization_id')])
        
        # Create participating organization relationships
        part_org_query = """
        UNWIND $batch AS aff
        MATCH (oa:OrganizationAffiliation {fhir_id: aff.fhir_id})
        MATCH (o:Organization {fhir_id: aff.participating_organization_id})
        MERGE (oa)-[:AFFILIATED_WITH]->(o)
        """
        session.run(part_org_query, batch=[a for a in affiliation_data if a.get('participating_organization_id')])
        
        # Create Location relationships
        loc_query = """
        UNWIND $batch AS aff
        MATCH (oa:OrganizationAffiliation {fhir_id: aff.fhir_id})
        UNWIND aff.location_ids AS loc_id
        MATCH (l:Location {fhir_id: loc_id})
        MERGE (oa)-[:AT_LOCATION]->(l)
        """
        session.run(loc_query, batch=[a for a in affiliation_data if a.get('location_ids')])
        
        # Create Endpoint relationships
        ep_query = """
        UNWIND $batch AS aff
        MATCH (oa:OrganizationAffiliation {fhir_id: aff.fhir_id})
        UNWIND aff.endpoint_ids AS ep_id
        MATCH (e:Endpoint {fhir_id: ep_id})
        MERGE (oa)-[:HAS_ENDPOINT]->(e)
        """
        session.run(ep_query, batch=[a for a in affiliation_data if a.get('endpoint_ids')])
