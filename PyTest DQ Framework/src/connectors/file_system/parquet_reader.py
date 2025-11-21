import os
import pandas as pd

class ParquetReader:

    def process(self, path: str, include_subfolders=True):
        dfs = []

        if include_subfolders:
            for root, _, files in os.walk(path):
                for name in files:
                    if name.endswith(".parquet"):
                        dfs.append(pd.read_parquet(os.path.join(root, name)))
        else:
            for name in os.listdir(path):
                if name.endswith(".parquet"):
                    dfs.append(pd.read_parquet(os.path.join(path, name)))

        if not dfs:
            raise ValueError(f"no parquet files found there: {path}")

        return pd.concat(dfs, ignore_index=True)
