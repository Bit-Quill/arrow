#!/usr/bin/env python3
#----------------------------------
# IMPORTS
#----------------------------------
import pyodbc
import pandas as pd
import argparse
import time
from os import environ
import statistics

#----------------------------------
# ARGUMENTS
#----------------------------------
parser = argparse.ArgumentParser(description="Benchmark ODBC Flight SQL Driver query performance against Dremio.")
parser.add_argument("--driver", required=True, help="ODBC Driver to use (must be pre-configured)")
parser.add_argument("--limit", type=int, default=1000, help="LIMIT for the SELECT query")
parser.add_argument("--iterations", type=int, default=5, help="Number of iterations to run")
args = parser.parse_args()

#----------------------------------
# SETUP
#----------------------------------
token = environ.get("token")
if not token:
    raise RuntimeError("Environment variable 'token' must be set.")

# Connector string
connector = (
    f"Driver={{{args.driver}}};"
    "ConnectionType=Direct;"
    "HOST=dremio-clients-demo.test.drem.io;"
    "PORT=32010;"
    "AuthenticationType=Plain;"
    f"UID=improving;PWD={token};ssl=true;"
)

#----------------------------------
# CREATE CONNECTION AND CURSOR
#----------------------------------

# Original
#cnxn = pyodbc.connect(connector, autocommit=True)
#cnxn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
#cursor = cnxn.cursor()

#
# Your Apache ODBC driver doesn’t yet fully support the W (wide/UTF-16) entry points → forcing ansi=True lets pyodbc stick to SQLExecDirectA etc.,
# which works around it.
# Now queries run to completion, but you’re seeing garbled characters in column names.
#
#cnxn = pyodbc.connect(connector, autocommit=True, ansi=False)
cnxn = pyodbc.connect(connector, autocommit=True, ansi=True)

# Ensure query text encoding works for drivers without SQLExecDirectW
cnxn.setencoding(encoding='utf-8')

# Handle both CHAR and WCHAR data returned from the driver
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
# Comment this out unless you confirm you need it:
# cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")

cursor = cnxn.cursor()

#----------------------------------
# RUN TEST
#----------------------------------
query = f'SELECT * FROM "Samples.samples.dremio.com"."NYC-taxi-trips-iceberg" LIMIT {args.limit}'

times_ms = []
column_types = None

for i in range(args.iterations):
    start = time.time()
    cursor.execute(query)
    rows = cursor.fetchall()
    elapsed_ms = (time.time() - start) * 1000
    times_ms.append(elapsed_ms)

    # grab column metadata once (first iteration is enough)
    if column_types is None:
        column_types = [
            (desc[0], desc[1])  # (column_name, sql_type)
            for desc in cursor.description
        ]

    print(f"Iteration {i+1}: {elapsed_ms:.2f} ms (rows returned: {len(rows)})")

#----------------------------------
# SUMMARY
#----------------------------------
avg_ms = statistics.mean(times_ms)
min_ms = min(times_ms)
max_ms = max(times_ms)

print("\n=== Benchmark Summary ===")
print(f"Driver used: {args.driver}")
print(f"Iterations: {args.iterations}")
print(f"Query LIMIT: {args.limit}")
print(f"Average time: {avg_ms:.2f} ms")
print(f"Shortest time: {min_ms:.2f} ms")
print(f"Longest time: {max_ms:.2f} ms")

# Report column-level SQL type usage
if column_types:
    print("\nColumn SQL Types:")
    for col_name, sql_type in column_types:
        if sql_type == pyodbc.SQL_CHAR:
            type_str = "SQL_CHAR (UTF-8)"
        elif sql_type == pyodbc.SQL_WCHAR:
            type_str = "SQL_WCHAR (UTF-16LE)"
        else:
            type_str = f"Other (SQL type code {sql_type})"
        print(f"  {col_name}: {type_str}")
