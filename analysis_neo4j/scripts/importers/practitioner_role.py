"""
PractitionerRole resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter, _to_json_string


class PractitionerRoleImporter(BaseImporter):
    """Import PractitionerRole resources into Neo4j."""
    
    RESOURCE_TYPE = "PractitionerRole"
    NODE_LABEL = "PractitionerRole"
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of PractitionerRole resources.
        
        Args:
            session: Neo4j session
            batch: List of PractitionerRole FHIR resources
        
        Returns:
            Number of nodes created/updated
        """
        # Prepare data for batch import
        role_data = []
        
        for resource in batch:
            fhir_id = resource.get('id')
            if not fhir_id:
                self._log(message=f"Skipping PractitionerRole without id: {resource}")
                continue
            
            # Extract identifiers as objects
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract single NPI for practitioner roles
            npi = self._extract_npi_single(identifiers=identifiers)
            
            # Extract practitioner reference
            practitioner_ref = resource.get('practitioner', {})
            practitioner_reference = practitioner_ref.get('reference') if isinstance(practitioner_ref, dict) else None
            practitioner_id = self._parse_reference(reference=practitioner_reference)
            
            # Extract organization reference
            organization_ref = resource.get('organization', {})
            organization_reference = organization_ref.get('reference') if isinstance(organization_ref, dict) else None
            organization_id = self._parse_reference(reference=organization_reference)
            
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
            
            # Extract specialty/taxonomy codes
            specialties = []
            for specialty in resource.get('specialty', []):
                if isinstance(specialty, dict):
                    coding = specialty.get('coding', [])
                    if coding and isinstance(coding, list):
                        for code in coding:
                            if isinstance(code, dict):
                                specialties.append({
                                    'system': code.get('system'),
                                    'code': code.get('code'),
                                    'display': code.get('display')
                                })
            
            # Extract active status
            active = resource.get('active')
            
            role_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'active': active,
                'practitioner_id': practitioner_id,
                'organization_id': organization_id,
                'location_ids': location_ids,
                'endpoint_ids': endpoint_ids,
                'specialties': [s['code'] for s in specialties if s.get('code')],
                'specialty_displays': [s['display'] for s in specialties if s.get('display')],
                'npi': npi,
                'identifiers': _to_json_string(obj=identifiers),
                'import_tag': self.import_tag,
            })
        
        # Batch import nodes - use CREATE or MERGE based on mode
        if self.use_create:
            query = """
            UNWIND $batch AS role
            CREATE (pr:PractitionerRole {
                fhir_id: role.fhir_id,
                import_tag: role.import_tag,
                resource_type: role.resource_type,
                active: role.active,
                practitioner_reference: role.practitioner_id,
                organization_reference: role.organization_id,
                location_references: role.location_ids,
                endpoint_references: role.endpoint_ids,
                specialties: role.specialties,
                specialty_displays: role.specialty_displays,
                npi: role.npi,
                identifiers: role.identifiers
            })
            RETURN count(pr) AS count
            """
        else:
            query = """
            UNWIND $batch AS role
            MERGE (pr:PractitionerRole {fhir_id: role.fhir_id})
            ON CREATE SET pr.import_tag = role.import_tag
            SET pr.resource_type = role.resource_type,
                pr.active = role.active,
                pr.practitioner_reference = role.practitioner_id,
                pr.organization_reference = role.organization_id,
                pr.location_references = role.location_ids,
                pr.endpoint_references = role.endpoint_ids,
                pr.specialties = role.specialties,
                pr.specialty_displays = role.specialty_displays,
                pr.npi = role.npi,
                pr.identifiers = role.identifiers
            RETURN count(pr) AS count
            """
        
        result = session.run(query, batch=role_data)
        record = result.single()
        node_count = record['count'] if record else 0
        
        # Create relationships
        self._create_relationships(session=session, role_data=role_data, use_create=self.use_create)
        
        return node_count
    
    @staticmethod
    def _create_relationships(*, session: Session, role_data: List[Dict[str, Any]], use_create: bool = False) -> None:
        """
        Create relationships between PractitionerRole and other resources.
        
        Args:
            session: Neo4j session
            role_data: List of processed role data
            use_create: If True, use CREATE instead of MERGE (faster but not idempotent)
        """
        if use_create:
            # FAST MODE: Single combined query to minimize round trips (4x faster!)
            combined_query = """
            UNWIND $batch AS role
            MATCH (pr:PractitionerRole {fhir_id: role.fhir_id})
            
            // Practitioner relationship
            OPTIONAL MATCH (p:Practitioner {fhir_id: role.practitioner_id})
            WHERE role.practitioner_id IS NOT NULL
            FOREACH (_ IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |
                CREATE (p)-[:HAS_ROLE]->(pr)
            )
            
            // Organization relationship
            WITH pr, role
            OPTIONAL MATCH (o:Organization {fhir_id: role.organization_id})
            WHERE role.organization_id IS NOT NULL
            FOREACH (_ IN CASE WHEN o IS NOT NULL THEN [1] ELSE [] END |
                CREATE (pr)-[:WORKS_AT]->(o)
            )
            
            // Location relationships (multiple)
            WITH pr, role
            UNWIND COALESCE(role.location_ids, []) AS loc_id
            OPTIONAL MATCH (l:Location {fhir_id: loc_id})
            FOREACH (_ IN CASE WHEN l IS NOT NULL THEN [1] ELSE [] END |
                CREATE (pr)-[:AT_LOCATION]->(l)
            )
            
            // Endpoint relationships (multiple)
            WITH DISTINCT pr, role
            UNWIND COALESCE(role.endpoint_ids, []) AS ep_id
            OPTIONAL MATCH (e:Endpoint {fhir_id: ep_id})
            FOREACH (_ IN CASE WHEN e IS NOT NULL THEN [1] ELSE [] END |
                CREATE (pr)-[:HAS_ENDPOINT]->(e)
            )
            """
            session.run(combined_query, batch=role_data)
            
        else:
            # SAFE MODE: Separate queries using MERGE (slower but clearer)
            # Create Practitioner relationships
            prac_query = """
            UNWIND $batch AS role
            MATCH (pr:PractitionerRole {fhir_id: role.fhir_id})
            MATCH (p:Practitioner {fhir_id: role.practitioner_id})
            MERGE (p)-[:HAS_ROLE]->(pr)
            """
            session.run(prac_query, batch=[r for r in role_data if r.get('practitioner_id')])
            
            # Create Organization relationships
            org_query = """
            UNWIND $batch AS role
            MATCH (pr:PractitionerRole {fhir_id: role.fhir_id})
            MATCH (o:Organization {fhir_id: role.organization_id})
            MERGE (pr)-[:WORKS_AT]->(o)
            """
            session.run(org_query, batch=[r for r in role_data if r.get('organization_id')])
            
            # Create Location relationships
            loc_query = """
            UNWIND $batch AS role
            MATCH (pr:PractitionerRole {fhir_id: role.fhir_id})
            UNWIND role.location_ids AS loc_id
            MATCH (l:Location {fhir_id: loc_id})
            MERGE (pr)-[:AT_LOCATION]->(l)
            """
            session.run(loc_query, batch=[r for r in role_data if r.get('location_ids')])
            
            # Create Endpoint relationships
            ep_query = """
            UNWIND $batch AS role
            MATCH (pr:PractitionerRole {fhir_id: role.fhir_id})
            UNWIND role.endpoint_ids AS ep_id
            MATCH (e:Endpoint {fhir_id: ep_id})
            MERGE (pr)-[:HAS_ENDPOINT]->(e)
            """
            session.run(ep_query, batch=[r for r in role_data if r.get('endpoint_ids')])
