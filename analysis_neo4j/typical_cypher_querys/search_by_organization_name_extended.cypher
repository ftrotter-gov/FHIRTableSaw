// Search by Organization Name - Extended (Two Hops)
// 
// Purpose: Find organizations by name and show their extended network (1-2 hops away).
//
// Usage: Replace 'Hospital' with the organization name or pattern you're searching for.
//
// Returns: A graph showing all organizations containing the specified name and 
// everything within 1-2 hops. Limited to 100 results to prevent overwhelming 
// the visualization.

MATCH (o:Organization)
WHERE o.name CONTAINS 'Hospital'
MATCH path = (o)-[*1..2]-(connected)
RETURN path
LIMIT 100;
