from __future__ import annotations

"""Exports BAföG pipeline output tables to Excel format."""

from misc.utility_functions import Literal, export_tables
from loaders.registry import LoaderRegistry
from pipeline.build import BafoegPipeline



ExportType = Literal["csv", "excel"]


class BafoegExcelExporter:
    def __init__(self, loaders: LoaderRegistry | None = None):
        self.loaders = loaders or LoaderRegistry()
        self.pipeline = BafoegPipeline(self.loaders)
        self.main_df = None

    def run(self):
        self.loaders.load_all()
        self.tables = self.pipeline.build()
        return self.tables


if __name__ == "__main__":
    calc = BafoegExcelExporter()
    tables = calc.run()          # returns the dict
    export_tables(tables, base_name="bafoeg_results")
