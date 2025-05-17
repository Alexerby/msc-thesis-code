from pathlib import Path
import pandas as pd
from loaders.registry import LoaderRegistry
import pandas as pd


class BafoegParquetLoader:
    def __init__(self, parquet_path: str):
        self.df = pd.read_parquet(parquet_path)
        self.registry = LoaderRegistry()
        self.merged_vars = []

    def merge_variable(self, key: str, columns: list[str], on: list[str] = ["pid", "syear"], how: str = "left", validate: bool = True):
        """Load variables from a SOEP source and merge into main df with optional validation."""
        pre_merge_len = len(self.df)

        df_new = self.registry.load(key)[on + columns].drop_duplicates(subset=on).copy()
        self.df = self.df.merge(df_new, on=on, how=how)

        self.merged_vars.extend(columns)

        if validate:
            post_merge_len = len(self.df)
            if post_merge_len > pre_merge_len:
                raise ValueError(f"Merge caused row inflation: {pre_merge_len} → {post_merge_len}. Possible key mismatch on {key}.")


    def merge_from_parquet(
        self,
        parquet_path: str,
        columns: list[str],
        on: list[str] = ["pid", "syear"],
        how: str = "left",
        validate: bool = True
    ):
        """Merge variables from an external parquet file."""
        pre_merge_len = len(self.df)

        df_new = pd.read_parquet(parquet_path)[on + columns].drop_duplicates(subset=on).copy()
        self.df = self.df.merge(df_new, on=on, how=how)
        self.merged_vars.extend(columns)

        if validate:
            post_merge_len = len(self.df)
            if post_merge_len > pre_merge_len:
                raise ValueError(
                    f"Merge caused row inflation: {pre_merge_len} → {post_merge_len}. "
                    f"Possible key mismatch with file: {parquet_path}."
                )

    def describe(self):
        print("DataFrame shape:", self.df.shape)
        print("Merged variables so far:", self.merged_vars)

    def get(self) -> pd.DataFrame:
        return self.df
