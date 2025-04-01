import os
from pathlib import Path
import pandas as pd

def write_size_csv(output_csv: str = "/home/ali/Desktop/1rbc/results/file_sizes.csv"):
    base_path = "/home/ali/Desktop/1rbc/data"
    formats = ["parquet", "delta", "orc"]
    scale_factors = ["sf10", "sf50", "sf100"]
    
    size_data = {"type": [], "sf10": [], "sf50": [], "sf100": []}
    
    for fmt in formats:
        size_data["type"].append(fmt)
        for sf in scale_factors:
            path = Path(f"{base_path}/{sf}/{fmt}")
            if fmt == "parquet":
                file_path = path / "data.parquet"
                size = os.path.getsize(file_path) if file_path.exists() else 0
            elif fmt == "delta":
                size = sum(os.path.getsize(f) for f in path.rglob("*") if f.is_file())
            elif fmt == "orc":
                orc_path = path / "data.orc"
                size = sum(os.path.getsize(f) for f in orc_path.rglob("*") if f.is_file()) if orc_path.exists() else 0
            
            size_str = f"{size / 1024**2:.2f} MB" if size < 1024**3 else f"{size / 1024**3:.2f} GB"
            size_data[sf].append(size_str)
    
    df = pd.DataFrame(size_data)
    df.to_csv(output_csv, index=False)
    print(f"CSV written to {output_csv}")


write_size_csv()