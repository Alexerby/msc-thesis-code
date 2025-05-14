import pandas as pd

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle
from pipeline.common.processor_deductions import apply_entity_allowances


def create_dataframe(
        siblings_df: pd.DataFrame,
        data: SOEPDataBundle, 
        policy: PolicyTableBundle,
        ) -> pd.DataFrame:
    """
    Builds a parent-level DataFrame with sociodemographic enrichment.

    Parameters
    ----------
    data : SOEPDataBundle
        Bundle of relevant SOEP Core DataFrames.

    Returns
    -------
    pd.DataFrame
        Enriched student-level dataset
    """

    df = base_view(siblings_df, policy)

    return (
        df 
    )



def base_view(siblings_df: pd.DataFrame, policy: PolicyTableBundle) -> pd.DataFrame:
    return (
        siblings_df
        .groupby(["student_pid", "syear"])
        .agg(
            joint_income=("excess_income", "sum"),
            num_siblings=("pid", "count"),
        )
        .reset_index()
    )
