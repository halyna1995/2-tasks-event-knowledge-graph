# -*- coding: utf-8 -*-
"""
Created on Sun Jul  7 12:46:10 2024

@author: shmot
"""
import os
import csv
from neo4j import GraphDatabase
# import time
# import psutil

# from contextlib import contextmanager  # Ensure this import is at the top

# @contextmanager
# def monitor_performance():
#     process = psutil.Process(os.getpid())
#     initial_mem = process.memory_info().rss / (1024 ** 2)
#     start_time = time.perf_counter()
#     try:
#         yield
#     finally:
#         end_time = time.perf_counter()
#         final_mem = process.memory_info().rss / (1024 ** 2)
#         peak_mem = process.memory_info().peak_wset / (1024 ** 2)  # Windows peak memory usage, for UNIX peak_rss
#         duration = end_time - start_time
#         memory_used = final_mem - initial_mem
#         print(f"Execution Time: {duration:.4f} seconds")
#         #print(f"Memory Used: {memory_used:.4f} MB (Final - Initial)")
#         print(f"Peak Memory Used: {peak_mem:.4f} MB")
#         with open('performance_log.txt', 'w') as file:
#             file.write(f"Execution Time: {duration:.4f} seconds\n")
#             #file.write(f"Memory Used: {memory_used:.4f} MB\n")
#             file.write(f"Peak Memory Used: {peak_mem:.4f} MB\n")


def read_csv(file_path):
    """Reads CSV file and returns a list of dictionaries."""
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]

def run_query(driver, query, parameters=None):
    with driver.session() as session:
        session.run(query, parameters)

def clear_database(driver):
    """Clears all data from the database."""
    run_query(driver, "MATCH (n) DETACH DELETE n")

def process_in_batches(driver, data, batch_size=500):
    """Process data in batches to manage memory usage."""
    total = len(data)
    with driver.session() as session:
        for start in range(0, total, batch_size):
            end = start + batch_size
            batch = data[start:end]
            # Important: Make sure each row has a valid 'id'
            batch = [row for row in batch if row.get('id')]  # Filter rows without 'id'
            if not batch:  # Skip package if all rows contain 'null' as 'id'
                continue
            query = """
            UNWIND $batch AS row
            MERGE (e:Event {id: row.id}) SET e += row.properties
            """
            session.run(query, {'batch': batch})
            
if __name__ == "__main__":
    uri = "bolt://localhost:7689"
    username = "neo4j"
    password = "12345678"
    driver = GraphDatabase.driver(uri, auth=(username, password))

    file_path = './prepared_logs/order_process_event_table_orderhandling_prepared.csv'
    if os.path.exists(file_path):
        data = read_csv(file_path)
        clear_database(driver)
        process_in_batches(driver, data)
        # with monitor_performance():  # We start monitoring
        driver.close()
    else:
        print("Error: File does not exist.")
