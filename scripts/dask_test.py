import dask.dataframe as dd
from deltalake import DeltaTable
import pandas as pd
import time

# Queries 
queries = {
    "Q1": lambda df: df["value"].max().compute(),
    "Q2": lambda df: df["value"].min().compute(),
    "Q3": lambda df: df["value"].mean().compute(),
    "Q4": lambda df: df[df["value"] > 20]["value"].count().compute(),
    "Q5": lambda df: df["value"].sum().compute()
}

# Benchmark
results = []
data_sizes = ["sf10", "sf50", "sf100"]
formats = ["parquet", "delta"]  # Skipping ORC due to pyarrow incompatibility
base_path = "/home/ali/Desktop/1rbc/data"

for size in data_sizes:
    for fmt in formats:
        for query_id, query_func in queries.items():
            try:
                start_time = time.time()
                
                if fmt == "parquet":
                    df = dd.read_parquet(f"{base_path}/{size}/parquet/data.parquet")
                elif fmt == "delta":
                    delta_table = DeltaTable(f"{base_path}/{size}/delta/")
                    pandas_df = delta_table.to_pandas()
                    df = dd.from_pandas(pandas_df, npartitions=4)
                
                # Execute query
                result = query_func(df)
                
                end_time = time.time()
                results.append({
                    "Tool": "Dask",
                    "Format": fmt,
                    "DataSize": size,
                    "Query": query_id,
                    "Time_ms": (end_time - start_time) * 1000
                })
            except Exception as e:
                print(f"Error processing {fmt} for {size}, {query_id}: {e}")

# Save results
df_results = pd.DataFrame(results)
df_results.to_csv("results/dask_query_times.csv", index=False)
print("Dask results saved to results/dask_query_times.csv")