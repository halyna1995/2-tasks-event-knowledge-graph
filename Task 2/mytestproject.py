# -*- coding: utf-8 -*-
"""
Created on Sun Jul  7 11:53:59 2024

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

# Configuration
uri = "bolt://localhost:7689"
username = "neo4j"
password = "12345678"
batch_size = 500  # Define the size of each batch

driver = GraphDatabase.driver(uri, auth=(username, password))

def run_query(driver, query, parameters=None):
    with driver.session() as session:
        session.run(query, parameters)

def read_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        return list(reader)

def process_batches(driver, data, headers):
    # Batch processing
    total = len(data)
    for start in range(0, total, batch_size):
        end = start + batch_size
        batch = data[start:end]
        query = """
        UNWIND $batch AS row
        CREATE (e:Event)
        SET e += row
        """
        params = {'batch': [{headers[i]: row[i] for i in range(len(headers))} for row in batch]}
        run_query(driver, query, params)

# Main execution
if __name__ == "__main__":
    input_path = './prepared_logs/'
    input_file = 'order_process_event_table_orderhandling_prepared.csv'
    file_path = os.path.join(input_path, input_file)
    data = read_csv(file_path)[1:]  # Skip header
    headers = read_csv(file_path)[0]

    # Clear existing data
    run_query(driver, "MATCH (n) DETACH DELETE n")

    # Process data in batches
    process_batches(driver, data, headers)
# with monitor_performance():  # We start monitoring
    # Close the database connection
    driver.close()
