// Search by NPI - Extended (Two Hops Away)
// 
// Purpose: Find any node (Practitioner or Organization) by NPI and show everything 
// connected within 1 to 2 relationships away.
//
// Usage: Replace '1234567890' with the actual NPI you're searching for.
//
// Returns: A graph showing the node with the specified NPI and all nodes within 
// 1-2 hops (relationships) away. This provides a broader view of the network.

MATCH (n)
WHERE n.npi = '1234567890'
MATCH path = (n)-[*1..2]-(connected)
RETURN path;
