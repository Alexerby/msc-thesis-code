from __future__ import annotations

"""Exports BAföG pipeline tables to .parquet format in ~/Downloads."""

import os
from loaders.registry import LoaderRegistry
from pipeline.build import BafoegPipeline


class BafoegParquetExporter:
    def __init__(self, loaders: LoaderRegistry | None = None, base_name: str = "parquets"):
        self.loaders = loaders or LoaderRegistry()
        self.pipeline = BafoegPipeline(self.loaders)
        self.base_name = base_name

        # Use user's Downloads directory
        downloads_dir = os.path.expanduser("~/Downloads/BAföG Results")
        self.output_dir = os.path.join(downloads_dir, base_name)
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self):
        self.loaders.load_all()
        self.tables = self.pipeline.build()
        return self.tables

    def export(self):
        for name, df in self.tables.items():
            out_path = os.path.join(self.output_dir, f"{name}.parquet")
            df.to_parquet(out_path, index=False)


if __name__ == "__main__":
    exporter = BafoegParquetExporter()
    exporter.run()
    exporter.export()
