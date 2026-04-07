// Search by NPI - With Relationship Labels (Alternative)
// 
// Purpose: Find any node (Practitioner or Organization) by NPI and show everything 
// directly connected, with explicit relationship labels visible in the graph.
//
// Usage: Replace '1234567890' with the actual NPI you're searching for.
//
// Returns: A graph showing the node with the specified NPI and all directly connected 
// nodes, with relationship types clearly labeled in the visualization.

MATCH (n)
WHERE n.npi = '1234567890'
MATCH path = (n)-[r]-(connected)
RETURN path;
