import duckdb
import time
import pandas as pd

# Connect to DuckDB and ensure extensions are loaded
con = duckdb.connect()
try:
    con.execute("INSTALL parquet;")
    con.execute("LOAD parquet;")
    # Test ORC support explicitly
    con.execute("SELECT * FROM read_orc('/home/ali/Desktop/1rbc/data/sf10/orc/data.orc') LIMIT 1")
    orc_supported = True
except Exception as e:
    print(f"ORC support failed: {e}. Falling back to Parquet-only.")
    orc_supported = False
con.close()

# Queries
queries = {
    "Q1": "SELECT MAX(value) as max_temp FROM temperatures",
    "Q2": "SELECT MIN(value) as min_temp FROM temperatures",
    "Q3": "SELECT AVG(value) as avg_temp FROM temperatures",
    "Q4": "SELECT COUNT(*) as hot_cities FROM temperatures WHERE value > 20",
    "Q5": "SELECT SUM(value) as sum_temp FROM temperatures"
}

# Benchmark
results = []
data_sizes = ["sf10", "sf50", "sf100"]
formats = ["parquet"] if not orc_supported else ["parquet", "orc"]
base_path = "/home/ali/Desktop/1rbc/data"

for size in data_sizes:
    for fmt in formats:
        con = duckdb.connect()
        con.execute("INSTALL parquet;")
        con.execute("LOAD parquet;")
        
        if fmt == "parquet":
            con.execute(f"CREATE TABLE temperatures AS SELECT * FROM parquet_scan('{base_path}/{size}/parquet/data.parquet')")
        elif fmt == "orc" and orc_supported:
            con.execute(f"CREATE TABLE temperatures AS SELECT * FROM read_orc('{base_path}/{size}/orc/data.orc')")
        
        for query_id, query_sql in queries.items():
            start_time = time.time()
            con.execute(query_sql).fetchall()
            end_time = time.time()
            results.append({
                "Tool": "DuckDB",
                "Format": fmt,
                "DataSize": size,
                "Query": query_id,
                "Time_ms": (end_time - start_time) * 1000
            })
        con.close()

# Save results
df_results = pd.DataFrame(results)
df_results.to_csv("results/duckdb_query_times.csv", index=False)
print("DuckDB results saved to results/duckdb_query_times.csv")