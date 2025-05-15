from numpy import where
import pandas as pd
from typing import Literal

def filter_years(df: pd.DataFrame, start_year: int = 2007, end_year: int = 2022) -> pd.DataFrame:
    """
    Filter DataFrame to keep only rows where 'syear' is within the given interval [start_year, end_year].
    
    Args:
        df (pd.DataFrame): Input DataFrame with a 'syear' column.
        start_year (int): Start year of the interval (inclusive). Defaults to 2008.
        end_year (int): End year of the interval (inclusive). Defaults to 2021.
    
    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    return df.loc[(df["syear"] >= start_year) & (df["syear"] <= end_year)].copy()


def filter_parents(
    df: pd.DataFrame,
    mode: Literal["either", "both"] = "either"
) -> pd.DataFrame:
    """
    Keep only those student‐rows for which parent IDs are available.

    - mode="either": keep rows where fnr>0 OR mnr>0  
    - mode="both"  : keep rows where fnr>0 AND mnr>0  

    (We treat any code ≤0—including –1, –2, or NaN—as “missing”.)
    """
    # sanity check
    if not {"fnr","mnr"}.issubset(df.columns):
        raise KeyError("`df` must contain `fnr` and `mnr` columns")

    has_fnr = df["fnr"].gt(0)
    has_mnr = df["mnr"].gt(0)

    if mode == "either":
        keep = has_fnr | has_mnr
    elif mode == "both":
        keep = has_fnr & has_mnr
    else:
        raise ValueError("mode must be 'either' or 'both'")

    return pd.DataFrame(df[keep].copy())


def filter_full_time_workers(
    df: pd.DataFrame,
    biol_df: pd.DataFrame,
    status_col: str = "lb0267_v1"
) -> pd.DataFrame:
    """
    Remove any person–year rows where the respondent reports being
    full-time employed (at least 35 h/week) according to biol/lb0267_v1 == 1,
    and print how many rows were dropped.
    """
    before = len(df)

    # 1) extract only the key + status column
    sub = biol_df[["pid", "syear", status_col]].copy()

    # 2) left-merge onto your main df
    merged = df.merge(sub, on=["pid", "syear"], how="left")

    # 3) coerce to integer, treat any non-1 (incl. NaN) as “not full-time”
    merged[status_col] = (
        pd.to_numeric(merged[status_col], errors="coerce")
          .fillna(0)
          .astype(int)
    )

    # 4) apply mask
    keep_mask = merged[status_col] != 1
    out = merged.loc[keep_mask, df.columns].copy()

    dropped = before - len(out)
    print("\n" * 2)
    print(f"Dropped {dropped} rows where {status_col} == 1 (full-time workers)")
    print("\n" * 2)

    return out
