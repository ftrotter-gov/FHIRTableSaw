# Example Cypher Queries for FHIR Graph Analysis

This document contains example queries to explore and validate the FHIR graph data in Neo4j.

## Table of Contents

- [Basic Counts and Verification](#basic-counts-and-verification)
- [NPI Queries](#npi-queries)
- [Network Analysis](#network-analysis)
- [Practitioner Queries](#practitioner-queries)
- [Organization Queries](#organization-queries)
- [Relationship Queries](#relationship-queries)
- [Geographic Queries](#geographic-queries)
- [Validation Queries](#validation-queries)

## Basic Counts and Verification

### Count all nodes by type

```cypher
MATCH (n)
RETURN labels(n) AS node_type, count(n) AS count
ORDER BY count DESC;
```

### Count all relationships by type

```cypher
MATCH ()-[r]->()
RETURN type(r) AS relationship_type, count(r) AS count
ORDER BY count DESC;
```

### Verify no OrganizationAffiliation misclassified as Organization

```cypher
// Should return 0
MATCH (n:Organization)
WHERE n.resource_type = "OrganizationAffiliation"
RETURN count(n) AS misclassified_count;
```

### Verify no PractitionerRole misclassified as Practitioner

```cypher
// Should return 0
MATCH (n:Practitioner)
WHERE n.resource_type = "PractitionerRole"
RETURN count(n) AS misclassified_count;
```

## NPI Queries

### Find all practitioners with NPIs

```cypher
MATCH (p:Practitioner)
WHERE p.npi IS NOT NULL
RETURN p.fhir_id, p.name, p.npi
LIMIT 25;
```

### Find all organizations with NPIs

```cypher
MATCH (o:Organization)
WHERE o.npi IS NOT NULL
RETURN o.fhir_id, o.name, o.npi
LIMIT 25;
```

### Search by specific NPI

```cypher
MATCH (n)
WHERE n.npi = "1234567890"
RETURN n;
```

### Count resources by identifier system

```cypher
MATCH (n)
WHERE n.identifier_systems IS NOT NULL
UNWIND n.identifier_systems AS system
RETURN system, count(*) AS count
ORDER BY count DESC
LIMIT 20;
```

## Network Analysis

### Find practitioner's complete network

```cypher
MATCH (p:Practitioner {fhir_id: "practitioner-123"})
OPTIONAL MATCH (p)-[:HAS_ROLE]->(pr:PractitionerRole)
OPTIONAL MATCH (pr)-[:WORKS_AT]->(o:Organization)
OPTIONAL MATCH (pr)-[:AT_LOCATION]->(l:Location)
OPTIONAL MATCH (pr)-[:HAS_ENDPOINT]->(e:Endpoint)
RETURN p, pr, o, l, e;
```

### Find all practitioners working at a specific organization

```cypher
MATCH (o:Organization {fhir_id: "org-123"})<-[:WORKS_AT]-(pr:PractitionerRole)<-[:HAS_ROLE]-(p:Practitioner)
RETURN p.name AS practitioner_name, p.npi AS npi, pr.specialties AS specialties
ORDER BY p.name;
```

### Find organization affiliations

```cypher
MATCH (o1:Organization)-[:HAS_AFFILIATION]->(oa:OrganizationAffiliation)-[:AFFILIATED_WITH]->(o2:Organization)
RETURN o1.name AS primary_org, oa.codes AS affiliation_type, o2.name AS participating_org
LIMIT 25;
```

### Find organization hierarchy

```cypher
MATCH path = (child:Organization)-[:PART_OF*1..5]->(parent:Organization)
RETURN child.name AS child_org, parent.name AS parent_org, length(path) AS levels
ORDER BY levels, child.name
LIMIT 25;
```

## Practitioner Queries

### Practitioners by specialty

```cypher
MATCH (p:Practitioner)-[:HAS_ROLE]->(pr:PractitionerRole)
WHERE ANY(spec IN pr.specialty_displays WHERE spec CONTAINS "Cardiology")
RETURN p.name, p.npi, pr.specialty_displays
LIMIT 25;
```

### Active practitioners with multiple roles

```cypher
MATCH (p:Practitioner)-[:HAS_ROLE]->(pr:PractitionerRole)
WHERE p.active = true
WITH p, count(pr) AS role_count
WHERE role_count > 1
RETURN p.name, p.npi, role_count
ORDER BY role_count DESC
LIMIT 25;
```

### Practitioners by gender distribution

```cypher
MATCH (p:Practitioner)
WHERE p.gender IS NOT NULL
RETURN p.gender, count(*) AS count
ORDER BY count DESC;
```

### Practitioners with specific qualifications

```cypher
MATCH (p:Practitioner)
WHERE ANY(qual IN p.qualifications WHERE qual CONTAINS "MD")
RETURN p.name, p.npi, p.qualifications
LIMIT 25;
```

## Organization Queries

### Organizations by type

```cypher
MATCH (o:Organization)
WHERE o.org_types IS NOT NULL
UNWIND o.org_types AS org_type
RETURN org_type, count(*) AS count
ORDER BY count DESC
LIMIT 20;
```

### Organizations by state

```cypher
MATCH (o:Organization)
WHERE o.states IS NOT NULL
UNWIND o.states AS state
RETURN state, count(*) AS count
ORDER BY count DESC;
```

### Organizations with endpoints

```cypher
MATCH (o:Organization)-[:HAS_ENDPOINT]->(e:Endpoint)
RETURN o.name, count(e) AS endpoint_count, collect(e.connection_type) AS endpoint_types
ORDER BY endpoint_count DESC
LIMIT 25;
```

### Organizations managing multiple locations

```cypher
MATCH (o:Organization)-[:MANAGES]->(l:Location)
WITH o, count(l) AS location_count
WHERE location_count > 1
RETURN o.name, o.npi, location_count
ORDER BY location_count DESC
LIMIT 25;
```

## Relationship Queries

### Find all relationships for a specific node

```cypher
MATCH (n {fhir_id: "some-fhir-id"})
OPTIONAL MATCH (n)-[r]-(connected)
RETURN n, type(r) AS relationship, labels(connected) AS connected_type, connected.fhir_id
LIMIT 50;
```

### Find shortest path between two practitioners

```cypher
MATCH (p1:Practitioner {fhir_id: "prac-1"}),
      (p2:Practitioner {fhir_id: "prac-2"}),
      path = shortestPath((p1)-[*..10]-(p2))
RETURN path;
```

### Find practitioners who share organizations

```cypher
MATCH (p1:Practitioner)-[:HAS_ROLE]->(:PractitionerRole)-[:WORKS_AT]->(o:Organization)
      <-[:WORKS_AT]-(:PractitionerRole)<-[:HAS_ROLE]-(p2:Practitioner)
WHERE p1.fhir_id < p2.fhir_id
RETURN p1.name AS practitioner_1, p2.name AS practitioner_2, o.name AS shared_org
LIMIT 25;
```

## Geographic Queries

### Locations by state

```cypher
MATCH (l:Location)
WHERE l.state IS NOT NULL
RETURN l.state, count(*) AS count
ORDER BY count DESC;
```

### Locations with coordinates

```cypher
MATCH (l:Location)
WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
RETURN l.name, l.city, l.state, l.latitude, l.longitude
LIMIT 25;
```

### Find locations near a coordinate (requires spatial plugin or approximation)

```cypher
// Simple bounding box approach (not true distance)
MATCH (l:Location)
WHERE l.latitude > 40.0 AND l.latitude < 41.0
  AND l.longitude > -75.0 AND l.longitude < -74.0
RETURN l.name, l.city, l.latitude, l.longitude
LIMIT 25;
```

## Validation Queries

### Find orphaned PractitionerRoles (no practitioner)

```cypher
MATCH (pr:PractitionerRole)
WHERE NOT EXISTS((pr)<-[:HAS_ROLE]-(:Practitioner))
RETURN pr.fhir_id, pr.practitioner_reference
LIMIT 25;
```

### Find orphaned OrganizationAffiliations

```cypher
MATCH (oa:OrganizationAffiliation)
WHERE NOT EXISTS((oa)<-[:HAS_AFFILIATION]-(:Organization))
  AND NOT EXISTS((oa)-[:AFFILIATED_WITH]->(:Organization))
RETURN oa.fhir_id, oa.organization_reference, oa.participating_organization_reference
LIMIT 25;
```

### Find nodes with missing expected properties

```cypher
// Practitioners without names
MATCH (p:Practitioner)
WHERE p.name IS NULL OR p.name = ""
RETURN p.fhir_id
LIMIT 25;
```

### Verify relationship integrity

```cypher
// Count how many relationships were successfully created vs referenced
MATCH (pr:PractitionerRole)
WITH count(pr) AS total_roles,
     sum(CASE WHEN pr.practitioner_reference IS NOT NULL THEN 1 ELSE 0 END) AS with_prac_ref,
     sum(CASE WHEN EXISTS((pr)<-[:HAS_ROLE]-(:Practitioner)) THEN 1 ELSE 0 END) AS with_prac_relationship
RETURN total_roles, with_prac_ref, with_prac_relationship,
       toFloat(with_prac_relationship) / with_prac_ref AS match_rate;
```

## Advanced Analysis Queries

### Find high-degree nodes (hubs)

```cypher
MATCH (n)
WHERE size((n)--()) > 10
RETURN labels(n) AS node_type, n.fhir_id, n.name, size((n)--()) AS degree
ORDER BY degree DESC
LIMIT 25;
```

### Community detection (organizations sharing many practitioners)

```cypher
MATCH (o1:Organization)<-[:WORKS_AT]-(:PractitionerRole)<-[:HAS_ROLE]-(p:Practitioner)
      -[:HAS_ROLE]->(:PractitionerRole)-[:WORKS_AT]->(o2:Organization)
WHERE o1.fhir_id < o2.fhir_id
WITH o1, o2, count(DISTINCT p) AS shared_practitioners
WHERE shared_practitioners > 5
RETURN o1.name, o2.name, shared_practitioners
ORDER BY shared_practitioners DESC
LIMIT 25;
```

### Endpoint usage analysis

```cypher
MATCH (e:Endpoint)
WITH e, size((e)<-[:HAS_ENDPOINT]-()) AS usage_count
RETURN e.connection_type, 
       count(*) AS endpoint_count,
       avg(usage_count) AS avg_usage,
       max(usage_count) AS max_usage
ORDER BY endpoint_count DESC;
```

---

## Tips for Query Optimization

1. Always use indexes - they're defined in `schema/indexes.cypher`
2. Use `EXPLAIN` or `PROFILE` to analyze query performance
3. Limit results during exploration with `LIMIT`
4. Use `WHERE` clauses to filter early in the query
5. Use `WITH` to break complex queries into steps

## Exporting Results

Export results to CSV:

```cypher
// In Neo4j Browser, you can click "Download" on results
// Or use cypher-shell with redirect:
cat query.cypher | cypher-shell -u neo4j -p password > results.csv
```
