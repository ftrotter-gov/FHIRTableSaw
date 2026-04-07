# Typical Graph Queries for Neo4j FHIR Analysis

This document contains common Cypher queries that return graph visualizations in Neo4j Browser.

All queries use the simple pattern of finding nodes matching your criteria and showing everything connected to them.

## Table of Contents

- [Search by NPI](#search-by-npi)
- [Search by Endpoint Domain](#search-by-endpoint-domain)
- [Search by Organization Name](#search-by-organization-name)
- [Search by Practitioner Last Name and State](#search-by-practitioner-last-name-and-state)

---

## Search by NPI

Find any node (Practitioner or Organization) by NPI and show everything connected.

### Simple: One Hop Away

```cypher
// Replace '1234567890' with the actual NPI
MATCH (n)
WHERE n.npi = '1234567890'
MATCH path = (n)--(connected)
RETURN path;
```

### Extended: Two Hops Away

```cypher
// Replace '1234567890' with the actual NPI
MATCH (n)
WHERE n.npi = '1234567890'
MATCH path = (n)-[*1..2]-(connected)
RETURN path;
```

### With Relationship Labels (Alternative)

```cypher
// Replace '1234567890' with the actual NPI
// This shows relationship types in the graph
MATCH (n)
WHERE n.npi = '1234567890'
MATCH path = (n)-[r]-(connected)
RETURN path;
```

---

## Search by Endpoint Domain

Find all endpoints matching a domain and show connections.

### Simple: Endpoints by Domain

```cypher
// Replace 'example.com' with the domain
MATCH (e:Endpoint)
WHERE e.address CONTAINS 'example.com'
MATCH path = (e)--(connected)
RETURN path;
```

### Extended: Show Full Network

```cypher
// Replace 'example.com' with the domain
MATCH (e:Endpoint)
WHERE e.address CONTAINS 'example.com'
MATCH path = (e)-[*1..2]-(connected)
RETURN path
LIMIT 100;
```

---

## Search by Organization Name

Find organizations by name and show their network.

### Simple: One Hop

```cypher
// Replace 'Hospital Name' with the organization name or pattern
MATCH (o:Organization)
WHERE o.name CONTAINS 'Hospital'
MATCH path = (o)--(connected)
RETURN path
LIMIT 50;
```

### Extended: Two Hops

```cypher
// Replace 'Hospital Name' with the organization name or pattern
MATCH (o:Organization)
WHERE o.name CONTAINS 'Hospital'
MATCH path = (o)-[*1..2]-(connected)
RETURN path
LIMIT 100;
```

---

## Search by Practitioner Last Name and State

Find practitioners by last name in a specific state.

State can be found in:
1. Practitioner's own address (stored in `states` array)
2. Location they work at (via PractitionerRole → Location)

### Simple: Practitioner with State in Address

```cypher
// Replace 'Smith' with last name and 'CA' with state code
MATCH (p:Practitioner)
WHERE p.name CONTAINS 'Smith'
  AND ANY(state IN p.states WHERE state = 'CA')
MATCH path = (p)--(connected)
RETURN path;
```

### Complex: Practitioner via Location State

```cypher
// Replace 'Smith' with last name and 'CA' with state code
// This finds practitioners working at locations in the specified state
MATCH (p:Practitioner)-[:HAS_ROLE]->(pr:PractitionerRole)-[:AT_LOCATION]->(l:Location)
WHERE p.name CONTAINS 'Smith'
  AND l.state = 'CA'
MATCH path = (p)--(connected)
RETURN path
LIMIT 50;
```

### Combined: State in Either Address OR Location

```cypher
// Replace 'Smith' with last name and 'CA' with state code
MATCH (p:Practitioner)
WHERE p.name CONTAINS 'Smith'
  AND (
    ANY(state IN p.states WHERE state = 'CA')
    OR EXISTS {
      MATCH (p)-[:HAS_ROLE]->(:PractitionerRole)-[:AT_LOCATION]->(l:Location)
      WHERE l.state = 'CA'
    }
  )
MATCH path = (p)-[*1..2]-(connected)
RETURN path
LIMIT 100;
```

---

## General Pattern

The general pattern for all these queries is:

```cypher
// 1. Find the nodes you want
MATCH (n:NodeType)
WHERE n.property = 'value'

// 2. Get everything connected (choose one):

// One hop away - MUST return path to show links!
MATCH path = (n)--(connected)
RETURN path;

// OR: Multiple hops (adjust *1..2 to control depth)
MATCH path = (n)-[*1..2]-(connected)
RETURN path;
```

**IMPORTANT:** Always `RETURN path` to show the relationships/links in the graph visualization!

---

## Tips

- **Use LIMIT** to prevent overwhelming visualizations
- **Adjust hop depth** `[*1..2]` means 1 to 2 hops; `[*1..3]` means 1 to 3 hops
- **Use CONTAINS** for partial matching on text fields
- **Use property matching** `{npi: 'value'}` for exact matches
- **View in Neo4j Browser** at <http://localhost:7474> for best graph visualization

---

## Quick Examples

### Find a specific NPI and its network

```cypher
MATCH (n) WHERE n.npi = '1234567890'
MATCH path = (n)-[*1..2]-(connected)
RETURN path;
```

### Find endpoints at a domain

```cypher
MATCH (e:Endpoint) WHERE e.address CONTAINS 'example.com'
MATCH path = (e)--(connected)
RETURN path;
```

### Find an organization and its network

```cypher
MATCH (o:Organization) WHERE o.name CONTAINS 'Hospital'
MATCH path = (o)--(connected)
RETURN path LIMIT 50;
```

### Find practitioners by name in a state

```cypher
MATCH (p:Practitioner)
WHERE p.name CONTAINS 'Smith' AND ANY(s IN p.states WHERE s = 'CA')
MATCH path = (p)--(connected)
RETURN path;
```
