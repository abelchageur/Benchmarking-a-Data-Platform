import polars as pl
import time
import pandas as pd

# Queries (Polars only supports Parquet)
queries = {
    "Q1": lambda df: df.select(pl.col("value").max()),
    "Q2": lambda df: df.select(pl.col("value").min()),
    "Q3": lambda df: df.select(pl.col("value").mean()),
    "Q4": lambda df: df.filter(pl.col("value") > 20).select(pl.count()),
    "Q5": lambda df: df.select(pl.col("value").sum())
}

# Benchmark
results = []
data_sizes = ["sf10", "sf50", "sf100"]
fmt = "parquet"  # Polars only works with Parquet
base_path = "/home/ali/Desktop/1rbc/data"

for size in data_sizes:
    df_pl = pl.read_parquet(f"{base_path}/{size}/parquet/data.parquet")
    for query_id, query_func in queries.items():
        start_time = time.time()
        query_func(df_pl)
        end_time = time.time()
        results.append({
            "Tool": "Polars",
            "Format": fmt,
            "DataSize": size,
            "Query": query_id,
            "Time_ms": (end_time - start_time) * 1000
        })

# Save results
df_results = pd.DataFrame(results)
df_results.to_csv("results/polars_query_times.csv", index=False)
print("Polars results saved to results/polars_query_times.csv")