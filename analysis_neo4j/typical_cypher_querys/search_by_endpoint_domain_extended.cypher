// Search by Endpoint Domain - Extended (Show Full Network)
// 
// Purpose: Find all Endpoint nodes matching a domain and show their extended network 
// (1-2 hops away).
//
// Usage: Replace 'example.com' with the domain you're searching for.
//
// Returns: A graph showing all endpoints containing the specified domain and 
// everything within 1-2 hops. Limited to 100 results to prevent overwhelming 
// the visualization.

MATCH (e:Endpoint)
WHERE e.address CONTAINS 'example.com'
MATCH path = (e)-[*1..2]-(connected)
RETURN path
LIMIT 100;
