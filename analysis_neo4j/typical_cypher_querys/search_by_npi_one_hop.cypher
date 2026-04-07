// Search by NPI - Simple (One Hop Away)
// 
// Purpose: Find any node (Practitioner or Organization) by NPI and show everything 
// directly connected (one relationship away).
//
// Usage: Replace '1234567890' with the actual NPI you're searching for.
//
// Returns: A graph showing the node with the specified NPI and all nodes directly 
// connected to it.

MATCH (n)
WHERE n.npi = '1234567890'
MATCH path = (n)--(connected)
RETURN path;
