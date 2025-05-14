import pandas as pd
import numpy as np

from .need_components import merge_need_amounts 
from .reported_amount import merge_reported_bafög

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle


def create_dataframe(
    df: pd.DataFrame, 
    policy: PolicyTableBundle,
    data: SOEPDataBundle,
    parents_joint_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create a DataFrame containing calculated BAföG need components.

    Parameters
    ----------
    df : pd.DataFrame
        Input student-level DataFrame with at least ['pid', 'syear', 'lives_at_home'].
    policy : PolicyTableBundle
        Bundle of statutory BAföG inputs (needs, insurance, etc.).
    data : SOEPDataBundle
        Bundle containing SOEP Core input DataFrames.
    parents_joint_df : pd.DataFrame
        Aggregated parental income per student (with excess income column).

    Returns
    -------
    pd.DataFrame
        DataFrame enriched with BAföG need components.
    """
    out = df.copy()

    # Step 1: Merge in base need, parental income, and reported BAföG
    out = merge_need_amounts(out, policy.needs, policy.insurance)
    out = merge_parental_excess_income(out, parents_joint_df)
    out = merge_reported_bafög(out, data)

    # Step 2: Optional column reordering
    cols = list(out.columns)
    if "excess_income_stu" in cols and "total_base_need" in cols:
        cols.remove("excess_income_stu")
        insert_at = cols.index("total_base_need") + 1
        cols.insert(insert_at, "excess_income_stu")
        out = out[cols]

    # Step 3: Fill missing excess incomes with 0 to avoid NaNs in calculation
    out["excess_income_stu"] = out["excess_income_stu"].fillna(0)
    out["excess_income_par"] = out["excess_income_par"].fillna(0)

    # Step 4: Compute theoretical BAföG award
    out["theoretical_bafög"] = np.maximum(
        out["total_base_need"] - (out["excess_income_stu"] + out["excess_income_par"]),
        0
    )

    return pd.DataFrame(out)



def merge_parental_excess_income(
    df: pd.DataFrame,
    parents_joint_df: pd.DataFrame
) -> pd.DataFrame:
    parents_trimmed = (
        parents_joint_df
        .rename(columns={
            "student_pid": "pid",         # align to student DataFrame key
            "excess_income": "excess_income_par"
        })
        .loc[:, ["pid", "syear", "excess_income_par"]]
    )

    return df.merge(
        parents_trimmed,
        on=["pid", "syear"],
        how="left"
    )
