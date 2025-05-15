import pandas as pd
import numpy as np

from .need_components import merge_need_amounts 
from .reported_amount import merge_reported_bafög
from .eligibility_conditions import is_ineligible

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle
from .data_cleaning import clean_bafög_columns



def create_dataframe(
    df: pd.DataFrame,  # minimal frame: just pid + syear
    *,
    students_df: pd.DataFrame,  # full student data for lookups
    policy: PolicyTableBundle,
    data: SOEPDataBundle,
    parents_joint_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create a DataFrame containing calculated BAföG need components.

    Parameters
    ----------
    df : pd.DataFrame
        Minimal student DataFrame with at least ['pid', 'syear'].
        Used as the base for the pipeline.
    students_df : pd.DataFrame
        Full student-level DataFrame with variables like 'lives_at_home', 'age', 'excess_income'.
    policy : PolicyTableBundle
        Statutory BAföG inputs (needs, insurance).
    data : SOEPDataBundle
        SOEP Core input DataFrames (e.g., for reported BAföG).
    parents_joint_df : pd.DataFrame
        Aggregated parental income per student.

    Returns
    -------
    pd.DataFrame
        Final student-year DataFrame with calculated BAföG components.
    """
    out = df.copy()

    # Merge in base need using statutory rules + household type
    out = merge_need_amounts(out, students_df, policy.needs, policy.insurance)

    # Merge in student and parental excess income
    out = merge_student_excess_income(out, students_df)
    out = merge_parental_excess_income(out, parents_joint_df)

    # Merge reported BAföG variables
    out = merge_reported_bafög(out, data)

    # Optional column reordering (place student excess after need)
    cols = list(out.columns)
    if "excess_income_stu" in cols and "total_base_need" in cols:
        cols.remove("excess_income_stu")
        insert_at = cols.index("total_base_need") + 1
        cols.insert(insert_at, "excess_income_stu")
        out = out[cols]

    # Fill missing values to avoid NaNs
    out["excess_income_stu"] = out["excess_income_stu"].fillna(0)
    out["excess_income_par"] = out["excess_income_par"].fillna(0)

    # Apply legal ineligibility logic centrally
    ineligible = is_ineligible(out, students_df)
    out["theoretical_bafög"] = np.where(
        ineligible,
        0,
        np.maximum(out["total_base_need"] - (out["excess_income_stu"] + out["excess_income_par"]), 0)
    )

    # Final eligibility flag: 1 if eligible, 0 otherwise
    out["theoretical_eligibility"] = (out["theoretical_bafög"] > 0).astype("int")

    # Final cleaning (includes boolean conversions, drops extras)
    out = clean_bafög_columns(out)

    return out



def merge_student_excess_income(
    df: pd.DataFrame,
    students_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge student-level excess income from the original students_df into the working pipeline df.

    Expects:
    - `students_df` contains 'excess_income'
    - Renames to 'excess_income_stu' for clarity
    """
    if "excess_income" not in students_df.columns:
        raise ValueError("`students_df` must contain 'excess_income'")

    return df.merge(
        students_df[["pid", "syear", "excess_income"]]
        .rename(columns={"excess_income": "excess_income_stu"}),
        on=["pid", "syear"],
        how="left",
        validate="one_to_one"
    )


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
