import pandas as pd 
import numpy as np
import sys


# def eligibility_age(df: pd.DataFrame, students_df: pd.DataFrame) -> pd.Series:
#     # TODO: This should be conditional on level of education 
#     # 30 up to masters, 35 is the eligibility condition for master studies
#     """
#     Returns a boolean Series marking students who are legally ineligible for BAföG due to age > 35.
#
#     Assumes:
#     - `df` contains ["pid", "syear"]
#     - `students_df` contains ["pid", "syear", "age"] (precomputed)
#
#     Returns:
#     - Boolean Series aligned with `df.index`
#     """
#     required_cols = {"pid", "syear"}
#     if not required_cols.issubset(df.columns):
#         raise ValueError(f"`df` must contain columns: {required_cols}")
#
#     if "age" not in students_df.columns:
#         raise ValueError("`students_df` must contain 'age' (precomputed)")
#
#     # Merge age onto df
#     merged = df.merge(
#         students_df[["pid", "syear", "age"]],
#         on=["pid", "syear"],
#         how="left",
#         validate="one_to_one"
#     )
#
#     return merged["age"] > 35


def eligibility_age_by_level(
    df: pd.DataFrame,
    students_df: pd.DataFrame,
    pl_df: pd.DataFrame,
    pgen_df: pd.DataFrame,
    *,
    age_col: str = "age",
    isced_col: str = "pgisced11"
) -> pd.Series:
    """
    Returns a boolean Series: True if legally ineligible by age,
    False if OK. Uses:
      • `students_df[age_col]` for age
      • `pgen_df[isced_col]` for highest attained degree
      • `pl_df` for current enrollment test

    Logic:
      - Not currently in higher ed → ineligible
      - If no prior tertiary degree (isced not in [6,7,8]) → treat as Bachelor‐track → max age 30
      - If has any tertiary degree (isced in [6,7,8]) → treat as Master‐track → max age 35
    """

    # Step 1: merge in age
    merged = df.merge(
        students_df[["pid", "syear", age_col]],
        on=["pid", "syear"],
        how="left"
    )

    # Step 2: merge in attained ISCED level
    merged = merged.merge(
        pgen_df[["pid", "syear", isced_col]],
        on=["pid", "syear"],
        how="left"
    )

    # Step 3: merge in enrollment flags
    merged = merged.merge(
        pl_df[["pid", "syear", "plg0012_h", "plg0014_v5", "plg0014_v6", "plg0014_v7"]],
        on=["pid", "syear"],
        how="left"
    )

    # Flag: in any education
    merged["plg0012_h"] = merged["plg0012_h"].fillna(0).astype(int)
    in_edu = merged["plg0012_h"] == 1

    # Flag: higher education in current year
    v5 = (merged["syear"].between(1999, 2008)) & merged["plg0014_v5"].isin([1, 2, 3]).fillna(False)
    v6 = (merged["syear"].between(2009, 2012)) & merged["plg0014_v6"].isin([1, 2, 3]).fillna(False)
    v7 = (merged["syear"] >= 2013)             & merged["plg0014_v7"].isin([1, 2, 3]).fillna(False)
    in_he = in_edu & (v5 | v6 | v7)

    # Flag: has tertiary degree
    has_tertiary = merged[isced_col].isin([6, 7, 8])
    master_track = in_he & has_tertiary

    # Compute ineligibility based on age threshold
    age = merged[age_col]
    thresh = np.where(master_track, 35, 30)

    # Final rule: over age limit or not in HE
    return (age > thresh) | (~in_he)


def is_ineligible(
    df: pd.DataFrame,
    students_df: pd.DataFrame,
    pl_df: pd.DataFrame,
    pgen_df: pd.DataFrame
) -> pd.Series:
    # age‐by‐level ineligibility


    age_flag = eligibility_age_by_level(df, students_df, pl_df, pgen_df)
    # (add more flags here as needed)
    return age_flag
