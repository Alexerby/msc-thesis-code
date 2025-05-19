from __future__ import annotations

"""Exports BAföG pipeline output tables to a single Excel file in ~/Downloads/BAföG Results."""

import os
from misc.utility_functions import Literal, export_tables
from loaders.registry import LoaderRegistry
from pipeline.build import BafoegPipeline


ExportType = Literal["csv", "excel"]


class BafoegExcelExporter:
    def __init__(self, loaders: LoaderRegistry | None = None, filename: str = "bafoeg_results.xlsx"):
        self.loaders = loaders or LoaderRegistry()
        self.pipeline = BafoegPipeline(self.loaders)
        self.filename = filename

        # Output path directly in ~/Downloads/BAföG Results
        downloads_dir = os.path.expanduser("~//Documents/MScEcon/Semester 2/Master Thesis I/Microsimulation")
        os.makedirs(downloads_dir, exist_ok=True)
        self.output_path = os.path.join(downloads_dir, filename)

    def run(self):
        self.loaders.load_all()
        self.tables = self.pipeline.build()
        return self.tables

    def export(self):
        export_tables(self.tables, output_path=self.output_path)


if __name__ == "__main__":
    exporter = BafoegExcelExporter()
    tables = exporter.run()
    exporter.export()
