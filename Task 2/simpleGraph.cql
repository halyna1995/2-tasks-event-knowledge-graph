// Creating nodes
CREATE (alice:Person {name: 'Alice', age: 30}),
       (bob:Person {name: 'Bob', age: 25}),
       (carol:Person {name: 'Carol', age: 33}),
       (wonderland:Country {name: 'Wonderland'}),
       (neverland:Country {name: 'Neverland'});

// # Creating a connection
MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), (carol:Person {name: 'Carol'}),
      (wonderland:Country {name: 'Wonderland'}), (neverland:Country {name: 'Neverland'})
CREATE (alice)-[:LIVES_IN]->(wonderland),
       (bob)-[:LIVES_IN]->(neverland),
       (alice)-[:KNOWS]->(bob),
       (alice)-[:WORKS_WITH]->(carol),
       (carol)-[:LIVES_IN]->(wonderland);

// Checking the creation of nodes and connections
MATCH (p:Person)
RETURN p.name AS Name, p.age AS Age;

MATCH (c:Country)
RETURN c.name AS Country;
// Display of all nodes and connections in the graph
MATCH (n)-[r]->(m)
RETURN n, r, m;

// Deleting nodes an  relationships
MATCH (p)
DETACH DELETE p;
