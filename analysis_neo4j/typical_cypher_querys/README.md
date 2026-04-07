# Typical Cypher Queries

This directory contains individual Cypher query files extracted from the main TypicalQuery.md documentation. Each file focuses on a specific type of search query with detailed comments.

## Query Files

### Search by NPI

- **search_by_npi_one_hop.cypher** - Find a node by NPI and show direct connections
- **search_by_npi_two_hops.cypher** - Find a node by NPI and show extended network (1-2 hops)
- **search_by_npi_with_relationship_labels.cypher** - Find a node by NPI with relationship types visible

### Search by Endpoint

- **search_by_endpoint_domain_simple.cypher** - Find endpoints by domain with direct connections
- **search_by_endpoint_domain_extended.cypher** - Find endpoints by domain with extended network

### Search by Organization

- **search_by_organization_name_simple.cypher** - Find organizations by name with direct connections
- **search_by_organization_name_extended.cypher** - Find organizations by name with extended network

### Search by Practitioner

- **search_practitioner_by_name_and_state_simple.cypher** - Find practitioners by name and state in their address
- **search_practitioner_by_name_and_location_state.cypher** - Find practitioners by name who work at locations in a state
- **search_practitioner_by_name_and_state_combined.cypher** - Find practitioners by name with state in either address OR location

## Usage

Each .cypher file contains:
- Descriptive comments explaining the purpose
- Usage instructions indicating what values to replace
- The Cypher query itself
- Information about what the query returns

To use a query:
1. Open the .cypher file
2. Read the comments to understand what it does
3. Replace placeholder values (like '1234567890', 'example.com', 'Smith', 'CA') with your actual search values
4. Copy and paste into Neo4j Browser at <http://localhost:7474>
5. Execute to see the graph visualization

## General Pattern

All queries follow this pattern:

```cypher
// 1. Find the nodes you want
MATCH (n:NodeType)
WHERE n.property = 'value'

// 2. Get everything connected
MATCH path = (n)--(connected)        // One hop
// OR
MATCH path = (n)-[*1..2]-(connected) // Multiple hops

// 3. Return the path to show relationships
RETURN path;
```

## Tips

- **Use LIMIT** to prevent overwhelming visualizations
- **Adjust hop depth** `[*1..2]` means 1 to 2 hops; `[*1..3]` means 1 to 3 hops
- **Use CONTAINS** for partial matching on text fields
- **Always RETURN path** to show the relationships/links in the graph visualization
- **View in Neo4j Browser** at <http://localhost:7474> for best graph visualization
