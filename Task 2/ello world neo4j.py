# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 21:36:44 2024

@author: shmot
"""

import pandas as pd
import neo4j
from neo4j import GraphDatabase


print("Pandas version:", pd.__version__)

uri = "bolt://localhost:7689"  # Змініть на вашу адресу та порт
username = "neo4j"  # Змініть на ваші облікові дані
password = "12345678"  # Змініть на ваш пароль

# Створення класу для з'єднання з базою даних
class HelloWorldExample:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from the Neo4j database!' AS greeting", message=message)
        return result.single()[0]

# Використання класу
greeter = HelloWorldExample(uri, username, password)
greeter.print_greeting("Hello world")
greeter.close()

