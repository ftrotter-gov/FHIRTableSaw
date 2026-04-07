// Search by Practitioner Last Name and State - Simple
// 
// Purpose: Find practitioners by last name who have a specific state in their own 
// address (stored in the states array property).
//
// Usage: 
//   - Replace 'Smith' with the last name you're searching for
//   - Replace 'CA' with the state code
//
// Returns: A graph showing all practitioners matching the name and state criteria 
// and everything directly connected to them.

MATCH (p:Practitioner)
WHERE p.name CONTAINS 'Smith'
  AND ANY(state IN p.states WHERE state = 'CA')
MATCH path = (p)--(connected)
RETURN path;
