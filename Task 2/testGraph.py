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

    def clear_graph(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

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
        self.clear_graph()  # Clear the graph before creating new nodes
        with self.driver.session() as session:
            # Creating nodes
            session.run("CREATE (p:Person {name: 'Alice'}), (c:Country {name: 'Wonderland'})")
            # Creating a connection
            session.run("MATCH (p:Person), (c:Country) WHERE p.name = 'Alice' AND c.name = 'Wonderland' CREATE (p)-[:LIVES_IN]->(c)")
            # Creation verification
            persons = session.run("MATCH (p:Person) RETURN p.name AS name").value()
            countries = session.run("MATCH (c:Country) RETURN c.name AS name").value()
            print("Persons:", persons)
            print("Countries:", countries)
            # Update nodes
            session.run("MATCH (p:Person {name: 'Alice'}) SET p.age = 30")
            # Checking for updates
            age = session.run("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age").single()[0]
            print("Alice's age:", age)
            # Deleting nodes and connections
            session.run("MATCH (p:Person)-[r:LIVES_IN]->(c:Country) DELETE p, c, r")
            # Deletion verification
            check = session.run("MATCH (p:Person) RETURN p").data()
            print("Remaining nodes after deletion:", check)
        with self.driver.session() as session:
            result = session.run("MATCH (n) RETURN n")
            for record in result:
                print(record["n"])


# Using the class
advanced_example = AdvancedNeo4jExample(uri, username, password)
advanced_example.create_greeting("Hello world")
advanced_example.create_and_check_nodes()
advanced_example.close()
