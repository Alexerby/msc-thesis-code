import numpy as np
import pandas as pd

def filter_years(df: pd.DataFrame, start_year: int = 2007, end_year: int = 2022) -> pd.DataFrame:
    #TODO: Currently seems data is filtered for 2009-2022 without this filter as well,
    # have we included data which is only available for these years?
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


def filter_students(df: pd.DataFrame, pl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the dataset to retain only individuals who are:
    - Currently in education (plg0012_h == 1)
    - Enrolled in higher education: university, applied sciences, or doctorate
      based on survey year:
        - plg0014_v5 for 1999–2008
        - plg0014_v6 for 2009–2012
        - plg0014_v7 for 2013–2022

    Parameters
    ----------
    df : pd.DataFrame
        Main DataFrame with person-year-level observations.
    pl_df : pd.DataFrame
        Supplementary person-level education data containing PL variables.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame containing only higher education students.
    """
    # Select relevant PL columns
    pl_vars = ["pid", "syear", "plg0012_h", "plg0014_v5", "plg0014_v6", "plg0014_v7"]
    pl_subset = pl_df[pl_vars].copy()

    # Merge PL data onto base DataFrame
    merged = df.merge(pl_subset, on=["pid", "syear"], how="left")

    # Currently in education
    currently_in_education = merged["plg0012_h"] == 1

    # Determine higher education enrollment by syear range
    is_univ_v5 = (merged["syear"].between(1999, 2008)) & (merged["plg0014_v5"].isin([1, 2, 3]))
    is_univ_v6 = (merged["syear"].between(2009, 2012)) & (merged["plg0014_v6"].isin([1, 2, 3]))
    is_univ_v7 = (merged["syear"] >= 2013) & (merged["plg0014_v7"].isin([1, 2, 3]))

    in_higher_education = is_univ_v5 | is_univ_v6 | is_univ_v7

    # Apply filters
    is_eligible = currently_in_education & in_higher_education
    filtered = merged[is_eligible]

    # Drop intermediate PL columns
    filtered = filtered.drop(columns=["plg0012_h", "plg0014_v5", "plg0014_v6", "plg0014_v7"])

    return pd.DataFrame(filtered)


def filter_age(df: pd.DataFrame, col_name: str, age_limit: int = 35):
    out = df.copy()
    out = out[out[col_name] < age_limit]
    return out
