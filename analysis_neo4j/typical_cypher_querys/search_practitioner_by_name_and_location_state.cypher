// Search by Practitioner Last Name and State - Complex (Via Location State)
// 
// Purpose: Find practitioners by last name who work at locations in a specific state.
// This searches through the relationship chain: 
// Practitioner → HAS_ROLE → PractitionerRole → AT_LOCATION → Location
//
// Usage: 
//   - Replace 'Smith' with the last name you're searching for
//   - Replace 'CA' with the state code
//
// Returns: A graph showing all practitioners matching the name who work at locations 
// in the specified state, and everything directly connected to them. Limited to 50 
// results to prevent overwhelming the visualization.

MATCH (p:Practitioner)-[:HAS_ROLE]->(pr:PractitionerRole)-[:AT_LOCATION]->(l:Location)
WHERE p.name CONTAINS 'Smith'
  AND l.state = 'CA'
MATCH path = (p)--(connected)
RETURN path
LIMIT 50;
