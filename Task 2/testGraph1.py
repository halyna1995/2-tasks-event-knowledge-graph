# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 22:45:15 2024

@author: shmot
"""

import pandas as pd
import neo4j
from neo4j import GraphDatabase

print("Pandas version:", pd.__version__)

uri = "bolt://localhost:7689"  # Change to your address and port
username = "neo4j"  # Change to your credentials
password = "12345678"  # Change to your password

class AdvancedNeo4jExample:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from the Neo4j database!' AS greeting", message=message)
        return result.single()[0]

    def create_and_check_nodes(self):
        with self.driver.session() as session:
            # Creating nodes
            session.run("""
            CREATE (alice:Person {name: 'Alice', age: 30}),
                   (bob:Person {name: 'Bob', age: 25}),
                   (carol:Person {name: 'Carol', age: 33}),
                   (wonderland:Country {name: 'Wonderland'}),
                   (neverland:Country {name: 'Neverland'})
            """)
            
            # Creating a connection
            session.run("""
            MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), (carol:Person {name: 'Carol'}),
                  (wonderland:Country {name: 'Wonderland'}), (neverland:Country {name: 'Neverland'})
            CREATE (alice)-[:LIVES_IN]->(wonderland),
                   (bob)-[:LIVES_IN]->(neverland),
                   (alice)-[:KNOWS]->(bob),
                   (alice)-[:WORKS_WITH]->(carol),
                   (carol)-[:LIVES_IN]->(wonderland)
            """)
            
            # Creation verification
            persons = session.run("MATCH (p:Person) RETURN p.name AS name").values()
            countries = session.run("MATCH (c:Country) RETURN c.name AS name").values()
            print("Persons:", [person[0] for person in persons])
            print("Countries:", [country[0] for country in countries])

# Using the class
advanced_example = AdvancedNeo4jExample(uri, username, password)
advanced_example.create_greeting("Hello world")
advanced_example.create_and_check_nodes()
advanced_example.close()
