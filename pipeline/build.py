"""
High-level pipeline that wires loaders, pure steps, and services.

This module defines the BafoegPipeline class, which orchestrates the
loading, filtering, and enrichment of SOEP student data for BAföG modeling.
"""

from __future__ import annotations

import pandas as pd

from loaders.registry import LoaderRegistry

# Modules that expose `create_dataframe()`
from . import students
from . import bafoeg_calculations
from . import parents
from . import siblings
from . import parents_joint
from . import siblings_joint


from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle


class BafoegPipeline:
    """Compose the final student DataFrame in one go."""


    def __init__(self, loaders: LoaderRegistry):
        self.loaders = loaders


    # ------------- public API -----------------
    def build(self) -> dict[str, pd.DataFrame]:
        # Load input data
        data = SOEPDataBundle.from_registry(self.loaders)
        policy = PolicyTableBundle.from_statutory_inputs()

        # STEP 1: Build students view
        students_df = self._build_students_df(data, policy)

        # STEP 2: Build siblings view (uncomment if you have logic)
        siblings_df = self._build_siblings_df(students_df, data, policy)
        siblings_joint_df = self._build_siblings_joint_df(siblings_df, data, policy)

        # STEP 3: Build parents view (optional - if siblings needed, pass that too)
        parents_df = self._build_parents_df(students_df, data, policy, siblings_df)
        parents_joint_df = self._build_parents_joint_df(parents_df, data, policy, siblings_joint_df)

        # STEP 4: Final BAföG calculation (based on students_df)
        bafoeg_df = self._build_bafoeg_df(students_df, data, policy, parents_joint_df)


        return {
            "students": students_df,
            "siblings": siblings_df,
            "siblings_joint": siblings_joint_df,
            "parents": parents_df,
            "parents_joint": parents_joint_df,
            "bafoeg_calculations": bafoeg_df,
        }


    # ------------- individual builders ---------------


    # Students 
    def _build_students_df(self, data: SOEPDataBundle, policy: PolicyTableBundle) -> pd.DataFrame:
        """Build the student-level base dataset (detached version, if needed)."""
        return students.create_dataframe(data, policy=policy)


    # Siblings 
    def _build_siblings_df(self, students_df, data: SOEPDataBundle, policy: PolicyTableBundle) -> pd.DataFrame:
        """
        all sibling-allowance logic lives here => num_eligible_siblings, sibling_allowance_total
        """
        return siblings.create_dataframe(students_df, data, policy)


    def _build_siblings_joint_df(self, siblings_df: pd.DataFrame, data: SOEPDataBundle, policy: PolicyTableBundle) -> pd.DataFrame:
        return siblings_joint.create_dataframe(
                siblings_df=siblings_df,
                data=data, 
                policy=policy
        )


    # Parents
    def _build_parents_df(self, students_df, data: SOEPDataBundle, policy: PolicyTableBundle, siblings_df: pd.DataFrame) -> pd.DataFrame:

        return parents.create_dataframe(
            students_df=students_df,
            data=data,
            policy=policy,
        )


    def _build_parents_joint_df(self, parents_df: pd.DataFrame, 
                                data: SOEPDataBundle, 
                                policy: PolicyTableBundle, 
                                siblings_df: pd.DataFrame) -> pd.DataFrame:
        return parents_joint.create_dataframe(
                parents_df=parents_df,
                data=data, 
                policy=policy,
                siblings_joint_df=siblings_df
        )
    

    # ------------- final BAföG calculation ------------
    def _build_bafoeg_df(
        self,
        base_df: pd.DataFrame, # which is the students_df
        data: SOEPDataBundle,
        policy: PolicyTableBundle,
        parents_joint_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Apply BAföG calculations to the student dataset."""
        cols = ["pid", "syear", "lives_at_home", "excess_income"]

        return (
            base_df.loc[:, cols].copy()
            .rename(columns={"excess_income": "excess_income_stu"})
            .pipe(
                bafoeg_calculations.create_dataframe,
                policy=policy,
                data=data,
                parents_joint_df = parents_joint_df
            )
        )
