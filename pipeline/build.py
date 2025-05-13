"""
High-level pipeline that wires loaders, pure steps, and services.

This module defines the BafoegPipeline class, which orchestrates the
loading, filtering, and enrichment of SOEP student data for BAföG modeling.
"""

from __future__ import annotations
from collections.abc import Callable

import pandas as pd

from data_handler import SOEPStatutoryInputs
from loaders.registry import LoaderRegistry
from services.tax import TaxService

# Dataframes
# These init files contain create_dataframe for each import
from . import students
from . import bafoeg_calculations


class BafoegPipeline:
    """Compose the final student DataFrame in one go."""

    def __init__(self, loaders: LoaderRegistry):
        self.loaders = loaders
        self.tax = TaxService()
        self._load_policy_tables()

    def build(self) -> dict[str, pd.DataFrame]:
        return {
            "students": self._build_students_df(),
            "bafoeg_calculations": self._build_bafoeg_df(),
        }

    def _build_students_df(self) -> pd.DataFrame:
        return students.create_dataframe(
            ppath_df=self.loaders.ppath(),
            region_df=self.loaders.region(),
            hgen_df=self.loaders.hgen(),
            bioparen_df=self.loaders.bioparen(),
            biol_df=self.loaders.biol(),
            pl_df=self.loaders.pl(),
        )

    def _build_bafoeg_df(self) -> pd.DataFrame:
        cols = ["pid", "syear", "lives_at_home"]
        base_df = self._build_students_df()
        return (
            base_df.loc[:, cols].copy()
            .pipe(bafoeg_calculations.create_dataframe, 
                  need_table = self._needs_table, 
                  insurance_table = self._insurance_table,
                  pl_df = self.loaders.pl()
                  )
        )

    def _load_policy_tables(self) -> None:
        self._werbung_df = self._load("Werbungskostenpauschale", ["Year", "werbungskostenpauschale"])
        self._allowance_table = self._load("Basic Allowances - § 25")
        self._needs_table = self._load("Basic Allowances - § 13")
        self._student_allowance_table = self._load("Basic Allowances - § 23")
        self._insurance_table = self._load("Basic Allowances - §13a")

    def _load(self, table_name: str, columns: list[str] | Callable = lambda _: True) -> pd.DataFrame:
        table = SOEPStatutoryInputs(table_name)
        table.load_dataset(columns=columns)
        return table.data.copy()
