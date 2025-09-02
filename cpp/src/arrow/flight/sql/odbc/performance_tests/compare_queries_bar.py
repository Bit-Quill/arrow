#!/usr/bin/env python3
import argparse
import subprocess
import csv
import json
import matplotlib.pyplot as plt

def run_query(driver, schema, table, query, iterations, label):
    cmd = [
        "python", "table_read_test.py",
        "--driver", driver,
        "--schema", schema,
        "--table", table,
        "--iterations", str(iterations),
        "--json",
        "--query", query
    ]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        full_output = []
        json_lines = []
        inside_json = False
        for line in proc.stdout:
            line = line.rstrip()
            full_output.append(line)
            print(line)
            if line.startswith("{"):
                inside_json = True
            if inside_json:
                json_lines.append(line)
            if line.endswith("}"):
                inside_json = False
        proc.wait()

        if not json_lines:
            print(f"\n--- FULL CHILD OUTPUT ({label}, {driver}) ---")
            print("\n".join(full_output))
            print("--- END CHILD OUTPUT ---\n")
            return None

        json_text = "\n".join(json_lines)
        return json.loads(json_text)

    except Exception as e:
        print(f"Error running {label} for {driver}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Compare two ODBC Flight SQL drivers on multiple queries")
    parser.add_argument("--driver_a", required=True, help="Driver A name")
    parser.add_argument("--driver_b", required=True, help="Driver B name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--iterations", type=int, default=5, help="Number of iterations per query")
    parser.add_argument("--outfile", default="compare_results.csv", help="CSV output file")
    parser.add_argument("--plotfile", default="compare_plot.png", help="Plot output file")
    args = parser.parse_args()

    # ----------------------------
    # 20 Queries
    # ----------------------------
    queries = {
        # Simple selects
        "Limit100": f'SELECT * FROM "{args.schema}"."{args.table}" LIMIT 100',
        "Limit1000": f'SELECT * FROM "{args.schema}"."{args.table}" LIMIT 1000',

        # Aggregations & Group By
        "AvgFareByPassenger": f'SELECT passenger_count, AVG(fare_amount) AS avg_fare FROM "{args.schema}"."{args.table}" GROUP BY passenger_count',
        "SumFareByPassenger": f'SELECT passenger_count, SUM(fare_amount) AS total_fare FROM "{args.schema}"."{args.table}" GROUP BY passenger_count',
        "AvgTipByPassenger": f'SELECT passenger_count, AVG(tip_amount) AS avg_tip FROM "{args.schema}"."{args.table}" GROUP BY passenger_count',
        "TotalAmountByPassenger": f'SELECT passenger_count, SUM(total_amount) AS total_amount FROM "{args.schema}"."{args.table}" GROUP BY passenger_count',

        # Filters
        "FareGreater50": f'SELECT * FROM "{args.schema}"."{args.table}" WHERE fare_amount > 50 LIMIT 500',
        "TripDistance5to10": f'SELECT * FROM "{args.schema}"."{args.table}" WHERE trip_distance_mi BETWEEN 5 AND 10 LIMIT 500',
        "PassengerCount2": f'SELECT * FROM "{args.schema}"."{args.table}" WHERE passenger_count = 2 LIMIT 500',
        "TipGreater10": f'SELECT * FROM "{args.schema}"."{args.table}" WHERE tip_amount > 10 LIMIT 500',

        # Ordering
        "OrderByFareDesc": f'SELECT * FROM "{args.schema}"."{args.table}" ORDER BY fare_amount DESC LIMIT 100',
        "OrderByTripDistance": f'SELECT * FROM "{args.schema}"."{args.table}" ORDER BY trip_distance_mi DESC LIMIT 100',

        # Aggregates without group by
        "TotalRowCount": f'SELECT COUNT(*) AS total_trips FROM "{args.schema}"."{args.table}"',
        "MaxFare": f'SELECT MAX(fare_amount) AS max_fare FROM "{args.schema}"."{args.table}"',
        "MinFare": f'SELECT MIN(fare_amount) AS min_fare FROM "{args.schema}"."{args.table}"',
        "MaxTripDistance": f'SELECT MAX(trip_distance_mi) AS max_distance FROM "{args.schema}"."{args.table}"',

        # Date/time functions
        "TripsByYear": f'SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, COUNT(*) AS trips FROM "{args.schema}"."{args.table}" GROUP BY year ORDER BY year',
        "TripsByMonth": f'SELECT EXTRACT(MONTH FROM pickup_datetime) AS month, COUNT(*) AS trips FROM "{args.schema}"."{args.table}" GROUP BY month ORDER BY month',

        # Window functions
        "TopFaresRanked": f'SELECT fare_amount, ROW_NUMBER() OVER (ORDER BY fare_amount DESC) AS rank FROM "{args.schema}"."{args.table}" LIMIT 100',
        "AvgFareByPassengerWindow": f'SELECT passenger_count, AVG(fare_amount) OVER (PARTITION BY passenger_count) AS avg_fare FROM "{args.schema}"."{args.table}" LIMIT 500'
    }

    results = []

    for label, query in queries.items():
        print(f"\nRunning {label}...")
        result_a = run_query(args.driver_a, args.schema, args.table, query, args.iterations, label)
        result_b = run_query(args.driver_b, args.schema, args.table, query, args.iterations, label)

        row = {"query": label}
        if result_a:
            row.update({
                "driver_a": args.driver_a,
                "a_avg": result_a["avg_ms"],
                "a_min": result_a["min_ms"],
                "a_max": result_a["max_ms"],
            })
        else:
            row.update({
                "driver_a": args.driver_a,
                "a_avg": "N/A",
                "a_min": "N/A",
                "a_max": "N/A",
            })

        if result_b:
            row.update({
                "driver_b": args.driver_b,
                "b_avg": result_b["avg_ms"],
                "b_min": result_b["min_ms"],
                "b_max": result_b["max_ms"],
            })
        else:
            row.update({
                "driver_b": args.driver_b,
                "b_avg": "N/A",
                "b_min": "N/A",
                "b_max": "N/A",
            })

        results.append(row)

    # ----------------------------
    # Write results to CSV
    # ----------------------------
    with open(args.outfile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "query", "driver_a", "a_avg", "a_min", "a_max",
            "driver_b", "b_avg", "b_min", "b_max"
        ])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"\nResults written to {args.outfile}")

    # ----------------------------
    # Plot results
    # ----------------------------
    queries_labels = [r["query"] for r in results]
    x = range(len(queries_labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(14, 6))

    driver_colors = {
        "a": "#1f77b4",
        "b": "#ff7f0e"
    }

    for i, r in enumerate(results):
        if r["a_avg"] != "N/A":
            a_mean = r["a_avg"]
            a_err = [[a_mean - r["a_min"]], [r["a_max"] - a_mean]]
            ax.bar(i - width/2, a_mean, width, yerr=a_err, capsize=5,
                   color=driver_colors["a"], alpha=0.8, label=args.driver_a if i == 0 else "")
        if r["b_avg"] != "N/A":
            b_mean = r["b_avg"]
            b_err = [[b_mean - r["b_min"]], [r["b_max"] - b_mean]]
            ax.bar(i + width/2, b_mean, width, yerr=b_err, capsize=5,
                   color=driver_colors["b"], alpha=0.8, label=args.driver_b if i == 0 else "")

    ax.set_ylabel("Execution time (ms)")
    ax.set_title("Query Performance Comparison: Driver A vs Driver B")
    ax.set_xticks(x)
    ax.set_xticklabels(queries_labels, rotation=45, ha="right")
    ax.legend(title="Driver")

    plt.tight_layout()
    plt.savefig(args.plotfile)
    print(f"Plot saved to {args.plotfile}")
    plt.show()


if __name__ == "__main__":
    main()
