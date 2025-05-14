import pandas as pd


def filter_age(df: pd.DataFrame, col_name: str, age_limit: int = 35):
    out = df.copy()
    out = out[out[col_name] < age_limit]
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



# def filter_parents(df: pd.DataFrame, sociodemographics: pd.DataFrame) -> pd.DataFrame:
#     """
#     Keep only individuals who are parents of students in the sample.
#
#     Parameters:
#     - df: Full ppath DataFrame
#     - sociodemographics: DataFrame containing fnr and mnr columns
#
#     Returns:
#     - Subset of df where pid is either an fnr or mnr of a student
#     """
#     valid_pids = sociodemographics[["fnr", "mnr"]].stack().dropna().unique().tolist()
#     return df[df["pid"].isin(valid_pids)].copy()
#
#
#
# def filter_zero_income_observations(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Removes rows where both excess_income_parents and excess_income_student are exactly 0.
#
#     Parameters:
#         df (pd.DataFrame): The DataFrame with income columns.
#
#     Returns:
#         pd.DataFrame: Filtered DataFrame excluding unrealistic zero-income cases.
#     """
#     condition = (df["excess_income_parents"] == 0) & (df["excess_income_student"] == 0)
#     return df[~condition].copy()
#
