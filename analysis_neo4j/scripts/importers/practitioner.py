"""
Practitioner resource importer.
"""

from typing import Dict, Any, List
from neo4j import Session
from .base import BaseImporter


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
            
            # Extract identifiers including NPI
            identifiers = self._extract_identifiers(resource=resource)
            
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
            
            # Extract qualification/credentials
            qualifications = []
            for qual in resource.get('qualification', []):
                if isinstance(qual, dict):
                    code = qual.get('code', {})
                    if isinstance(code, dict):
                        coding = code.get('coding', [])
                        if coding and isinstance(coding, list):
                            qualifications.append(coding[0].get('display', coding[0].get('code', '')))
            
            practitioner_data.append({
                'fhir_id': fhir_id,
                'resource_type': self.RESOURCE_TYPE,
                'name': name_str,
                'active': active,
                'gender': gender,
                'npi': identifiers['npi'],
                'identifier_systems': identifiers['identifier_systems'],
                'identifier_values': identifiers['identifier_values'],
                'qualifications': qualifications,
                'import_tag': self.import_tag,
            })
        
        # Batch import using MERGE for idempotency
        query = """
        UNWIND $batch AS prac
        MERGE (p:Practitioner {fhir_id: prac.fhir_id})
        ON CREATE SET p.import_tag = prac.import_tag
        SET p.resource_type = prac.resource_type,
            p.name = prac.name,
            p.active = prac.active,
            p.gender = prac.gender,
            p.npi = prac.npi,
            p.identifier_systems = prac.identifier_systems,
            p.identifier_values = prac.identifier_values,
            p.qualifications = prac.qualifications
        RETURN count(p) AS count
        """
        
        result = session.run(query, batch=practitioner_data)
        record = result.single()
        return record['count'] if record else 0
