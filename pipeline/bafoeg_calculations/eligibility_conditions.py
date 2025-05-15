import pandas as pd 
import numpy as np


def eligibility_age(df: pd.DataFrame, students_df: pd.DataFrame) -> pd.Series:
    # TODO: This should be conditional on level of education 
    # 30 up to masters, 35 is the eligibility condition for master studies
    """
    Returns a boolean Series marking students who are legally ineligible for BAföG due to age > 35.

    Assumes:
    - `df` contains ["pid", "syear"]
    - `students_df` contains ["pid", "syear", "age"] (precomputed)

    Returns:
    - Boolean Series aligned with `df.index`
    """
    required_cols = {"pid", "syear"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"`df` must contain columns: {required_cols}")

    if "age" not in students_df.columns:
        raise ValueError("`students_df` must contain 'age' (precomputed)")

    # Merge age onto df
    merged = df.merge(
        students_df[["pid", "syear", "age"]],
        on=["pid", "syear"],
        how="left",
        validate="one_to_one"
    )

    return merged["age"] > 35


def is_ineligible(df: pd.DataFrame, students_df: pd.DataFrame) -> pd.Series:
    """
    Returns a boolean Series for legal ineligibility based on combined conditions:
    - Age > 30 (default)
    - [Placeholder for future: foreign status, asset limit, etc.]
    """
    flags = []

    # Age-based ineligibility
    # This evaluates to true if age > 0.
    flags.append(eligibility_age(df, students_df))

    # [Future]: Add other flags here, e.g.:
    # flags.append(eligibility_foreign(df, students_df))
    # flags.append(eligibility_assets(df, students_df))

    # Will return True if any of the appended flags (booleans) are true 
    # marking the individual as ineligible
    return np.logical_or.reduce(flags)
