// Search by Endpoint Domain - Simple
// 
// Purpose: Find all Endpoint nodes matching a domain and show their direct connections.
//
// Usage: Replace 'example.com' with the domain you're searching for.
//
// Returns: A graph showing all endpoints containing the specified domain and 
// everything directly connected to them.

MATCH (e:Endpoint)
WHERE e.address CONTAINS 'example.com'
MATCH path = (e)--(connected)
RETURN path;
