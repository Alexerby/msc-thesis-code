from __future__ import annotations

"""Minimal façade ‑ constructs the pipeline and exports the result."""

from misc.utility_functions import Literal, export_tables
from loaders.registry import LoaderRegistry
from pipeline.build import BafoegPipeline

import pandas as pd


ExportType = Literal["csv", "excel"]


class BafoegCalculator:
    def __init__(self, loaders: LoaderRegistry | None = None):
        self.loaders = loaders or LoaderRegistry()
        self.pipeline = BafoegPipeline(self.loaders)
        self.main_df = None

    def run(self):
        self.loaders.load_all()
        self.tables = self.pipeline.build()
        return self.tables

    # def export(self, path: str = "bafoeg.xlsx"):
    #     with pd.ExcelWriter(path, engine="xlsxwriter") as xl:
    #         for name, frame in self.tables.items():
    #             frame.to_excel(xl, sheet_name=name, index=False)


if __name__ == "__main__":
    calc = BafoegCalculator()
    tables = calc.run()          # returns the dict
    export_tables(tables, base_name="bafoeg_results")
