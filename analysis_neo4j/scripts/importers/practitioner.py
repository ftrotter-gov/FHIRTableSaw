"""
Practitioner resource importer.
"""

from typing import Dict, Any, List, Optional
from neo4j import Session
from .base import BaseImporter, _to_json_string


class PractitionerImporter(BaseImporter):
    """Import Practitioner resources into Neo4j."""
    
    RESOURCE_TYPE = "Practitioner"
    NODE_LABEL = "Practitioner"
    
    def import_batch(self, *, session: Session, batch: List[Dict[str, Any]]) -> int:
        """
        Import a batch of Practitioner resources.
        
        Args:
            session: Neo4j session
            batch: List of Practitioner FHIR resources
        
        Returns:
            Number of nodes created/updated
        """
        # Prepare data for batch import
        practitioner_data = []
        
        for resource in batch:
            fhir_id = resource.get('id')
            if not fhir_id:
                self._log(message=f"Skipping Practitioner without id: {resource}")
                continue
            
            # Extract identifiers as objects
            identifiers = self._extract_identifiers(resource=resource)
            
            # Extract single NPI for practitioners
            npi = self._extract_npi_single(identifiers=identifiers)
            
            # Extract name (use first name entry)
            name_str = None
            names = resource.get('name', [])
            if names and isinstance(names, list) and len(names) > 0:
                name_obj = names[0]
                family = name_obj.get('family', '')
                given = name_obj.get('given', [])
                given_str = ' '.join(given) if isinstance(given, list) else str(given)
                name_str = f"{given_str} {family}".strip()
            
            # Extract other key fields
            active = resource.get('active')
            gender = resource.get('gender')
            
            # Extract addresses as coherent objects
            addresses = self._extract_addresses(resource=resource)
            
            # Extract telecoms separated by type
            telecoms = self._extract_telecoms(resource=resource)
            
            # Extract qualifications (specialties, degrees, licenses)
            qual_data = self._extract_qualifications(resource=resource)
            
            # Extract languages spoken (non-English only)
            languages_spoken = self._extract_languages(resource=resource)
            
            # Extract endpoint references with ranks from extensions
            endpoint_refs = self._extract_endpoint_references(resource=resource)
            
            practitioner_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'name': name_str,
                'active': active,
                'gender': gender,
                'npi': npi,
                'identifiers': _to_json_string(obj=identifiers),
                'addresses': _to_json_string(obj=addresses),
                'emails': telecoms['emails'],
                'phones': telecoms['phones'],
                'faxes': telecoms['faxes'],
                'specialties': qual_data['specialties'],
                'degrees': qual_data['degrees'],
                'licenses': _to_json_string(obj=qual_data['licenses']),
                'languages_spoken': languages_spoken,
                'endpoint_references': endpoint_refs,
                'import_tag': self.import_tag,
            })
        
        if not practitioner_data:
            return 0
        
        # Use CREATE or MERGE based on mode
        if self.use_create:
            # Fast mode: CREATE (not idempotent, but much faster)
            query = """
            UNWIND $batch AS prac
            CREATE (p:Practitioner {
                fhir_id: prac.fhir_id,
                import_tag: prac.import_tag,
                resource_type: prac.resource_type,
                name: prac.name,
                active: prac.active,
                gender: prac.gender,
                npi: prac.npi,
                identifiers: prac.identifiers,
                addresses: prac.addresses,
                emails: prac.emails,
                phones: prac.phones,
                faxes: prac.faxes,
                specialties: prac.specialties,
                degrees: prac.degrees,
                licenses: prac.licenses,
                languages_spoken: prac.languages_spoken
            })
            RETURN count(p) AS count
            """
        else:
            # Safe mode: MERGE (idempotent, but slower)
            query = """
            UNWIND $batch AS prac
            MERGE (p:Practitioner {fhir_id: prac.fhir_id})
            ON CREATE SET p.import_tag = prac.import_tag
            SET p.resource_type = prac.resource_type,
                p.name = prac.name,
                p.active = prac.active,
                p.gender = prac.gender,
                p.npi = prac.npi,
                p.identifiers = prac.identifiers,
                p.addresses = prac.addresses,
                p.emails = prac.emails,
                p.phones = prac.phones,
                p.faxes = prac.faxes,
                p.specialties = prac.specialties,
                p.degrees = prac.degrees,
                p.licenses = prac.licenses,
                p.languages_spoken = prac.languages_spoken
            RETURN count(p) AS count
            """
        
        result = session.run(query, batch=practitioner_data)
        record = result.single()
        return record['count'] if record else 0
    
    @staticmethod
    def _extract_qualifications(*, resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract qualifications into specialties, degrees, and licenses.
        
        Args:
            resource: The FHIR Practitioner resource
        
        Returns:
            Dictionary with specialties, degrees, and licenses lists
        """
        specialties = set()  # Use set to deduplicate
        degrees = []
        licenses = []
        
        qualifications = resource.get('qualification', [])
        if not isinstance(qualifications, list):
            qualifications = [qualifications] if qualifications else []
        
        for qual in qualifications:
            if not isinstance(qual, dict):
                continue
            
            code = qual.get('code', {})
            if not isinstance(code, dict):
                continue
            
            coding = code.get('coding', [])
            if not isinstance(coding, list):
                continue
            
            for code_entry in coding:
                if not isinstance(code_entry, dict):
                    continue
                
                system = code_entry.get('system', '')
                display = code_entry.get('display', '')
                
                # Specialty: http://nucc.org/provider-taxonomy
                if system == 'http://nucc.org/provider-taxonomy' and display:
                    specialties.add(display)
                
                # Degree: http://terminology.hl7.org/CodeSystem/v2-0360
                elif system == 'http://terminology.hl7.org/CodeSystem/v2-0360' and display:
                    degrees.append(display)
            
            # Extract license if present
            license_info = PractitionerImporter._extract_license_from_qualification(qualification=qual)
            if license_info:
                licenses.append(license_info)
        
        return {
            'specialties': sorted(list(specialties)),  # Convert set to sorted list
            'degrees': degrees,
            'licenses': licenses
        }
    
    @staticmethod
    def _extract_license_from_qualification(*, qualification: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Extract license information from a qualification block.
        
        Args:
            qualification: A qualification dictionary
        
        Returns:
            Dictionary with state and license_number, or None
        """
        # Check if this qualification has an identifier array
        identifiers = qualification.get('identifier', [])
        if not isinstance(identifiers, list):
            return None
        
        for identifier in identifiers:
            if not isinstance(identifier, dict):
                continue
            
            # Check if this is a medical license
            id_type = identifier.get('type', {})
            if isinstance(id_type, dict):
                coding = id_type.get('coding', [])
                if isinstance(coding, list):
                    for code in coding:
                        if isinstance(code, dict) and code.get('code') == 'MD':
                            # This is a medical license
                            license_number = identifier.get('value')
                            if license_number:
                                # Extract state from issuer
                                issuer = qualification.get('issuer', {})
                                if isinstance(issuer, dict):
                                    issuer_ref = issuer.get('reference')
                                    state = BaseImporter._extract_state_from_org_reference(reference=issuer_ref)
                                    if state:
                                        return {
                                            'state': state,
                                            'license_number': license_number
                                        }
        
        return None
    
    @staticmethod
    def _extract_languages(*, resource: Dict[str, Any]) -> List[str]:
        """
        Extract non-English languages from communication array.
        
        Args:
            resource: The FHIR Practitioner resource
        
        Returns:
            List of language names (non-English only)
        """
        languages = []
        
        communications = resource.get('communication', [])
        if not isinstance(communications, list):
            communications = [communications] if communications else []
        
        for comm in communications:
            if not isinstance(comm, dict):
                continue
            
            coding = comm.get('coding', [])
            if not isinstance(coding, list):
                continue
            
            for code_entry in coding:
                if not isinstance(code_entry, dict):
                    continue
                
                language_code = code_entry.get('code', '')
                display = code_entry.get('display', '')
                
                # Skip English
                if language_code.lower() != 'en' and display:
                    languages.append(display)
        
        return languages
    
    @staticmethod
    def _extract_endpoint_references(*, resource: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract endpoint references with ranks from extensions.
        
        Args:
            resource: The FHIR Practitioner resource
        
        Returns:
            List of endpoint references with {fhir_id, rank}
        """
        endpoint_refs = []
        
        extensions = resource.get('extension', [])
        if not isinstance(extensions, list):
            extensions = [extensions] if extensions else []
        
        for ext in extensions:
            if not isinstance(ext, dict):
                continue
            
            # Check if this is an endpoint reference extension
            url = ext.get('url', '')
            if 'base-ext-endpoint-reference' not in url:
                continue
            
            # Extract nested extension array
            nested_extensions = ext.get('extension', [])
            if not isinstance(nested_extensions, list):
                continue
            
            endpoint_id = None
            rank = None
            
            for nested_ext in nested_extensions:
                if not isinstance(nested_ext, dict):
                    continue
                
                nested_url = nested_ext.get('url', '')
                
                if nested_url == 'endpoint':
                    # Extract endpoint reference
                    value_ref = nested_ext.get('valueReference', {})
                    if isinstance(value_ref, dict):
                        reference = value_ref.get('reference')
                        endpoint_id = BaseImporter._parse_reference(reference=reference)
                
                elif nested_url == 'rank':
                    # Extract rank
                    rank = nested_ext.get('valuePositiveInt')
            
            if endpoint_id:
                endpoint_refs.append({
                    'fhir_id': endpoint_id,
                    'rank': rank
                })
        
        return endpoint_refs
    
    @staticmethod
    def _create_endpoint_relationships(*, session: Session, practitioner_data: List[Dict[str, Any]], use_create: bool = False) -> None:
        """
        Create relationships between Practitioner and Endpoints with rank property.
        
        Args:
            session: Neo4j session
            practitioner_data: List of processed practitioner data
            use_create: If True, use CREATE instead of MERGE (faster but not idempotent)
        """
        # Prepare data for relationship creation
        relationship_data = []
        
        for prac in practitioner_data:
            endpoint_refs = prac.get('endpoint_references', [])
            if not endpoint_refs:
                continue
            
            for ep_ref in endpoint_refs:
                relationship_data.append({
                    'practitioner_id': prac['fhir_id'],
                    'endpoint_id': ep_ref['fhir_id'],
                    'rank': ep_ref.get('rank')
                })
        
        if not relationship_data:
            return
        
        # Choose operation based on mode
        if use_create:
            query = """
            UNWIND $batch AS rel
            MATCH (p:Practitioner {fhir_id: rel.practitioner_id})
            MATCH (e:Endpoint {fhir_id: rel.endpoint_id})
            CREATE (p)-[r:HAS_ENDPOINT]->(e)
            SET r.rank = rel.rank
            """
        else:
            query = """
            UNWIND $batch AS rel
            MATCH (p:Practitioner {fhir_id: rel.practitioner_id})
            MATCH (e:Endpoint {fhir_id: rel.endpoint_id})
            MERGE (p)-[r:HAS_ENDPOINT]->(e)
            SET r.rank = rel.rank
            """
        
        session.run(query, batch=relationship_data)
