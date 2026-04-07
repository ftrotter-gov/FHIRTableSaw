// Search by NPI - One Hop Plus Bridge Nodes
// 
// Purpose: Starting from a single NPI, find all meaningfully connected nodes by:
//   1. Showing all directly connected nodes (one hop)
//   2. Treating PractitionerRole and OrganizationAffiliation as "transparent bridges"
//      and showing what's connected through them (two hops when middle node is a bridge)
//
// This query recognizes that PractitionerRole and OrganizationAffiliation are really
// complex edges in the graph rather than meaningful destinations themselves.
//
// Usage: Replace '1234567890' with the actual NPI you're searching for.
//
// Returns: A graph showing:
//   - The node with the specified NPI
//   - All directly connected nodes (1 hop)
//   - All nodes connected through PractitionerRole or OrganizationAffiliation (2 hops)
//   - The bridge nodes themselves (PractitionerRole/OrganizationAffiliation)
//
// This provides a clearer view of what an NPI is actually connected to in terms of
// Organizations, Locations, Endpoints, and other Practitioners.

MATCH (n)
WHERE n.npi = '1234567890'

// Get direct connections (one hop to anything)
OPTIONAL MATCH path1 = (n)--(connected1)

// Get connections through PractitionerRole (two hops)
OPTIONAL MATCH path2 = (n)--(pr:PractitionerRole)--(connected2)

// Get connections through OrganizationAffiliation (two hops)
OPTIONAL MATCH path3 = (n)--(oa:OrganizationAffiliation)--(connected3)

RETURN path1, path2, path3;
