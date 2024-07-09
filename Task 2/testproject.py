# -*- coding: utf-8 -*-
"""
Created on Sun Jul  7 12:46:10 2024

@author: shmot
"""
import os
import csv
import time
import psutil
from neo4j import GraphDatabase
from contextlib import contextmanager  # Ensure this import is at the top

@contextmanager
def monitor_performance():
    process = psutil.Process(os.getpid())
    initial_mem = process.memory_info().rss / (1024 ** 2)
    start_time = time.perf_counter()
    try:
        yield
    finally:
        end_time = time.perf_counter()
        final_mem = process.memory_info().rss / (1024 ** 2)
        peak_mem = process.memory_info().peak_wset / (1024 ** 2)  # Windows peak memory usage, for UNIX peak_rss
        duration = end_time - start_time
        memory_used = final_mem - initial_mem
        print(f"Execution Time: {duration:.4f} seconds")
        #print(f"Memory Used: {memory_used:.4f} MB (Final - Initial)")
        print(f"Peak Memory Used: {peak_mem:.4f} MB")
        with open('performance_log.txt', 'w') as file:
            file.write(f"Execution Time: {duration:.4f} seconds\n")
            #file.write(f"Memory Used: {memory_used:.4f} MB\n")
            file.write(f"Peak Memory Used: {peak_mem:.4f} MB\n")


def read_csv(file_path):
    """Reads CSV file and returns headers and data as a list."""
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)  # Read the first line as headers
        print("Headers found:", headers)  # Debugging line to check headers
        data = [row for row in reader]  # Read the rest as data
    return headers, data

def run_query(driver, query, parameters=None):
    with driver.session() as session:
        session.run(query, parameters)

def clear_database(driver):
    """Clears all data from the database."""
    run_query(driver, "MATCH (n) DETACH DELETE n")

def process_in_batches(driver, data, headers, batch_size=500):
    """Process data in batches to manage memory usage and ensure data integrity."""
    total = len(data)
    with driver.session() as session:
        for start in range(0, total, batch_size):
            end = start + batch_size
            batch = data[start:end]
            # Using EventID as an identifier
            filtered_batch = [
                {headers[i]: value for i, value in enumerate(row) if value}  # Filtering empty values
                for row in batch
                if row[headers.index('EventID')] is not None  # Make sure EventID is not null
            ]
            if not filtered_batch:
                continue  # We skip the package if all entries are invalid
            query = """
            UNWIND $batch AS row
            MERGE (e:Event {EventID: row.EventID})
            SET e += row
            """
            session.run(query, {'batch': filtered_batch})

def create_relationships(driver):
    """Creates connections based on node properties"""
    query = """
    MATCH (e1:Event), (e2:Event)
    WHERE e1.Order = e2.Order AND e1.EventID < e2.EventID
    CREATE (e1)-[:RELATED]->(e2)
    """
    run_query(driver, query)



if __name__ == "__main__":
    uri = "bolt://localhost:7689"
    username = "neo4j"
    password = "12345678"
    driver = GraphDatabase.driver(uri, auth=(username, password))

    file_path = './prepared_logs/order_process_event_table_orderhandling_prepared.csv'
    if os.path.exists(file_path):
        headers, data = read_csv(file_path)
        if 'EventID' not in headers:
            print("Error: 'EventID' column is missing from the CSV file.")
        else:
              with monitor_performance():  # Windows peak memory usage, for UNIX peak_rss
                  clear_database(driver)
                  process_in_batches(driver, data, headers)
                  create_relationships(driver)
                  driver.close()
    else:
        print("Error: File does not exist.")
