#!/usr/bin/env python3
import argparse
import pyodbc
import time
import json
import csv
import sys
from os import environ

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark ODBC Flight SQL Driver query performance."
    )
    parser.add_argument("--driver", required=True, help="ODBC driver name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--limit", type=int, help="LIMIT for SELECT query (ignored if --query provided)")
    parser.add_argument("--iterations", type=int, default=1, help="Number of iterations to run")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument("--csv", help="Output CSV file")
    parser.add_argument("--query", type=str, help="Optional SQL query to run")
    args = parser.parse_args()

    token = environ.get("token")
    if not token:
        raise RuntimeError("Environment variable 'token' must be set.")

    # Build SQL query
    if args.query:
        sql = args.query
    else:
        limit = args.limit if args.limit else 100
        sql = f'SELECT * FROM "{args.schema}"."{args.table}" LIMIT {limit}'

    # Connection string for Arrow Flight SQL ODBC
    conn_str = (
        f"Driver={{{args.driver}}};"
        "ConnectionType=Direct;"
        "HOST=dremio-clients-demo.test.drem.io;"
        "PORT=32010;"
        "AuthenticationType=Plain;"
        f"UID=improving;PWD={token};ssl=true;"
        "DisableCertificateVerification=1;"
    )

    # Connect
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
    except pyodbc.Error as e:
        print(f"Failed to connect using driver '{args.driver}': {e}")
        sys.exit(1)

    # UTF-8 decoding for CHAR columns
    conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
    cursor = conn.cursor()

    # Run benchmark
    timings = []
    rows_returned = []
    for i in range(args.iterations):
        start = time.time()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            elapsed_ms = (time.time() - start) * 1000
            timings.append(elapsed_ms)
            rows_returned.append(len(rows))
            print(f"Iteration {i+1}: {elapsed_ms:.2f} ms (rows returned: {len(rows)})")
        except pyodbc.Error as e:
            print(f"Query execution failed: {e}")
            timings.append(None)
            rows_returned.append(None)

    valid_timings = [t for t in timings if t is not None]
    avg_ms = sum(valid_timings)/len(valid_timings) if valid_timings else None
    min_ms = min(valid_timings) if valid_timings else None
    max_ms = max(valid_timings) if valid_timings else None

    result = {
        "driver": args.driver,
        "schema": args.schema,
        "table": args.table,
        "iterations": args.iterations,
        "query": sql,
        "avg_ms": avg_ms,
        "min_ms": min_ms,
        "max_ms": max_ms,
        "all_runs_ms": timings,
        "rows_returned": rows_returned,
    }

    # JSON output
    if args.json:
        print(json.dumps(result, indent=2))

    # CSV output
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(result.keys())
            writer.writerow(result.values())

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
