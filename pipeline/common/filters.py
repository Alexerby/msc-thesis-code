import pandas as pd


def filter_age(df: pd.DataFrame, col_name: str, min_age: int = None, max_age: int = None):
    out = df.copy()
    if min_age is not None:
        out = out[out[col_name] >= min_age]
    if max_age is not None:
        out = out[out[col_name] <= max_age]
    return out


def filter_students(
    df: pd.DataFrame,
    pl_df: pd.DataFrame,
    negate: bool = False
) -> pd.DataFrame:
    """
    Filters the dataset to retain only individuals who are:
    - Currently in education (plg0012_h == 1)
    - Enrolled in higher education: university, applied sciences, or doctorate

    Set `negate=True` to retain only individuals who are NOT higher education students.

    Parameters
    ----------
    df : pd.DataFrame
        Main DataFrame with person-year-level observations.
    pl_df : pd.DataFrame
        Supplementary person-level education data containing PL variables.
    negate : bool, optional
        If True, filters for non-students instead. Default is False.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
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

    is_student = currently_in_education & in_higher_education

    # Apply filter or its negation
    final = merged[~is_student if negate else is_student]

    # Drop intermediate PL columns
    final = final.drop(columns=["plg0012_h", "plg0014_v5", "plg0014_v6", "plg0014_v7"])

    return pd.DataFrame(final)




def drop_zero_rows(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Remove all rows where parental excess income is zero.
    
    Parameters:
    - df: A pandas DataFrame containing the 'excess_income_par' column.
    
    Returns:
    - A cleaned DataFrame with only rows where excess_income_par ≠ 0.
    """
    # Filter out rows where excess_income_par is 0
    df_cleaned = df[df[col_name] != 0].copy()
    
    return pd.DataFrame(df_cleaned)
