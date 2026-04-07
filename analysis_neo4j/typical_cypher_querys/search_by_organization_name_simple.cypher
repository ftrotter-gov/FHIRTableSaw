// Search by Organization Name - Simple (One Hop)
// 
// Purpose: Find organizations by name and show their direct connections.
//
// Usage: Replace 'Hospital' with the organization name or pattern you're searching for.
//
// Returns: A graph showing all organizations containing the specified name and 
// everything directly connected to them. Limited to 50 results to prevent 
// overwhelming the visualization.

MATCH (o:Organization)
WHERE o.name CONTAINS 'Hospital'
MATCH path = (o)--(connected)
RETURN path
LIMIT 50;
