#!/usr/bin/env python3
import subprocess
import argparse
import json
import matplotlib.pyplot as plt
import csv

# ----------------------------
# ARGUMENTS
# ----------------------------
parser = argparse.ArgumentParser(description="Compare two ODBC Flight SQL drivers using JSON output.")
parser.add_argument("--driver_a", required=True, help="First driver name")
parser.add_argument("--driver_b", required=True, help="Second driver name")
parser.add_argument("--iterations", type=int, default=5, help="Number of iterations per limit")
parser.add_argument("--limits", type=int, nargs="+", default=[100, 1000, 10000], help="List of LIMIT values")
parser.add_argument("--outfile", default="perf_results.csv", help="CSV output filename")
parser.add_argument("--plotfile", default="perf_plot.png", help="Plot output filename")
args = parser.parse_args()

# ----------------------------
# HELPER FUNCTION TO RUN DRIVER
# ----------------------------
def run_driver(driver_name):
    results = {}
    for limit in args.limits:
        cmd = [
            "python", "table_read_test.py",
            "--driver", driver_name,
            "--limit", str(limit),
            "--iterations", str(args.iterations),
            "--json"
        ]
        print(f"\nRunning {driver_name} with LIMIT={limit}...")

        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = proc.stdout.strip()
            # Extract JSON from output
            json_start = output.find("{")
            json_end = output.rfind("}") + 1
            json_text = output[json_start:json_end]
            data = json.loads(json_text)
            avg_ms = data["avg_ms"]
            min_ms = data["min_ms"]
            max_ms = data["max_ms"]
            results[limit] = (avg_ms, min_ms, max_ms)
        except subprocess.CalledProcessError as e:
            print(f"Error running {driver_name} with LIMIT={limit}:\n{e.stdout}\n{e.stderr}")
            results[limit] = (None, None, None)
        except Exception as e:
            print(f"Failed to parse JSON for {driver_name} with LIMIT={limit}: {e}")
            results[limit] = (None, None, None)

    return results

# ----------------------------
# RUN DRIVERS
# ----------------------------
driver_a_results = run_driver(args.driver_a)
driver_b_results = run_driver(args.driver_b)

# ----------------------------
# SAVE RESULTS TO CSV
# ----------------------------
with open(args.outfile, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Driver", "Limit", "Avg_ms", "Min_ms", "Max_ms"])
    for limit, (avg, min_, max_) in driver_a_results.items():
        writer.writerow([args.driver_a, limit, avg, min_, max_])
    for limit, (avg, min_, max_) in driver_b_results.items():
        writer.writerow([args.driver_b, limit, avg, min_, max_])
print(f"Results saved to {args.outfile}")

# ----------------------------
# PLOT RESULTS WITH ERROR BARS
# ----------------------------
limits = args.limits
avg_a = [driver_a_results[l][0] for l in limits]
avg_b = [driver_b_results[l][0] for l in limits]

err_a = [[avg_a[i] - driver_a_results[l][1] if driver_a_results[l][1] else 0 for i, l in enumerate(limits)],
         [driver_a_results[l][2] - avg_a[i] if driver_a_results[l][2] else 0 for i, l in enumerate(limits)]]

err_b = [[avg_b[i] - driver_b_results[l][1] if driver_b_results[l][1] else 0 for i, l in enumerate(limits)],
         [driver_b_results[l][2] - avg_b[i] if driver_b_results[l][2] else 0 for i, l in enumerate(limits)]]

plt.figure(figsize=(10,6))
plt.errorbar(limits, avg_a, yerr=err_a, fmt='o-', capsize=5, label=args.driver_a)
plt.errorbar(limits, avg_b, yerr=err_b, fmt='s-', capsize=5, label=args.driver_b)
plt.xlabel("LIMIT value")
plt.ylabel("Query time (ms)")
plt.title("ODBC Flight SQL Driver Performance Comparison")
plt.legend()
plt.grid(True)
plt.xscale("log")
plt.yscale("log")
plt.savefig(args.plotfile)
print(f"Plot saved to {args.plotfile}")
plt.show()

