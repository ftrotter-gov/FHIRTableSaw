// Neo4j Schema and Indexes for FHIR Resources
// Run this after starting Neo4j and before importing data

// ============================================
// Constraints (enforce uniqueness)
// ============================================

// Practitioner constraints
CREATE CONSTRAINT practitioner_fhir_id IF NOT EXISTS
FOR (p:Practitioner) REQUIRE p.fhir_id IS UNIQUE;

// PractitionerRole constraints
CREATE CONSTRAINT practitioner_role_fhir_id IF NOT EXISTS
FOR (pr:PractitionerRole) REQUIRE pr.fhir_id IS UNIQUE;

// Organization constraints
CREATE CONSTRAINT organization_fhir_id IF NOT EXISTS
FOR (o:Organization) REQUIRE o.fhir_id IS UNIQUE;

// OrganizationAffiliation constraints
CREATE CONSTRAINT organization_affiliation_fhir_id IF NOT EXISTS
FOR (oa:OrganizationAffiliation) REQUIRE oa.fhir_id IS UNIQUE;

// Endpoint constraints
CREATE CONSTRAINT endpoint_fhir_id IF NOT EXISTS
FOR (e:Endpoint) REQUIRE e.fhir_id IS UNIQUE;

// Location constraints
CREATE CONSTRAINT location_fhir_id IF NOT EXISTS
FOR (l:Location) REQUIRE l.fhir_id IS UNIQUE;

// ============================================
// Indexes for FHIR Identifiers
// ============================================

// Index on all NPI values (most important for healthcare queries)
CREATE INDEX npi_practitioner IF NOT EXISTS
FOR (p:Practitioner) ON (p.npi);

CREATE INDEX npi_organization IF NOT EXISTS
FOR (o:Organization) ON (o.npi);

// Index on identifier arrays (for quick identifier lookups)
CREATE INDEX identifier_system_practitioner IF NOT EXISTS
FOR (p:Practitioner) ON (p.identifier_systems);

CREATE INDEX identifier_system_organization IF NOT EXISTS
FOR (o:Organization) ON (o.identifier_systems);

CREATE INDEX identifier_system_location IF NOT EXISTS
FOR (l:Location) ON (l.identifier_systems);

CREATE INDEX identifier_system_endpoint IF NOT EXISTS
FOR (e:Endpoint) ON (e.identifier_systems);

// ============================================
// Indexes for Common Query Patterns
// ============================================

// Active status for filtering
CREATE INDEX active_practitioner IF NOT EXISTS
FOR (p:Practitioner) ON (p.active);

CREATE INDEX active_organization IF NOT EXISTS
FOR (o:Organization) ON (o.active);

CREATE INDEX active_practitioner_role IF NOT EXISTS
FOR (pr:PractitionerRole) ON (pr.active);

// Name searches
CREATE TEXT INDEX name_practitioner IF NOT EXISTS
FOR (p:Practitioner) ON (p.name);

CREATE TEXT INDEX name_organization IF NOT EXISTS
FOR (o:Organization) ON (o.name);

// ============================================
// Indexes for Reference Resolution
// ============================================

// These help speed up relationship creation during import
CREATE INDEX reference_practitioner IF NOT EXISTS
FOR (pr:PractitionerRole) ON (pr.practitioner_reference);

CREATE INDEX reference_organization IF NOT EXISTS
FOR (pr:PractitionerRole) ON (pr.organization_reference);

CREATE INDEX reference_participating_org IF NOT EXISTS
FOR (oa:OrganizationAffiliation) ON (oa.participating_organization_reference);

CREATE INDEX reference_primary_org IF NOT EXISTS
FOR (oa:OrganizationAffiliation) ON (oa.organization_reference);

// ============================================
// Indexes for Import Tracking
// ============================================

// Import tag indexes for filtering by import run
CREATE INDEX import_tag_practitioner IF NOT EXISTS
FOR (p:Practitioner) ON (p.import_tag);

CREATE INDEX import_tag_organization IF NOT EXISTS
FOR (o:Organization) ON (o.import_tag);

CREATE INDEX import_tag_location IF NOT EXISTS
FOR (l:Location) ON (l.import_tag);

CREATE INDEX import_tag_endpoint IF NOT EXISTS
FOR (e:Endpoint) ON (e.import_tag);

CREATE INDEX import_tag_practitioner_role IF NOT EXISTS
FOR (pr:PractitionerRole) ON (pr.import_tag);

CREATE INDEX import_tag_organization_affiliation IF NOT EXISTS
FOR (oa:OrganizationAffiliation) ON (oa.import_tag);
