"""
High-level pipeline that wires loaders, pure steps, and services.

This module defines the BafoegPipeline class, which orchestrates the
loading, filtering, and enrichment of SOEP student data for BAföG modeling.
"""

from __future__ import annotations

import pandas as pd

from loaders.registry import LoaderRegistry
from services.tax import TaxService

# Modules that expose `create_dataframe()`
from . import students
from . import bafoeg_calculations

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle


class BafoegPipeline:
    """Compose the final student DataFrame in one go."""


    def __init__(self, loaders: LoaderRegistry):
        self.loaders = loaders
        self.tax = TaxService()

    def build(self) -> dict[str, pd.DataFrame]:
        # Load data from SOEP and policy sources
        data = SOEPDataBundle.from_registry(self.loaders)
        policy = PolicyTableBundle.from_statutory_inputs()

        # Compose final tables
        students_df = students.create_dataframe(data)
        bafoeg_df = self._build_bafoeg_df(students_df, data, policy)

        return {
            "students": students_df,
            "bafoeg_calculations": bafoeg_df,
        }


    def _build_students_df(self, data: SOEPDataBundle) -> pd.DataFrame:
        """Build the student-level base dataset (detached version, if needed)."""
        return students.create_dataframe(data)


    def _build_bafoeg_df(
        self,
        base_df: pd.DataFrame,
        data: SOEPDataBundle,
        policy: PolicyTableBundle
    ) -> pd.DataFrame:
        """Apply BAföG calculations to the student dataset."""
        cols = ["pid", "syear", "lives_at_home"]
        return (
            base_df.loc[:, cols].copy()
            .pipe(
                bafoeg_calculations.create_dataframe,
                policy=policy,
                data=data
            )
        )
