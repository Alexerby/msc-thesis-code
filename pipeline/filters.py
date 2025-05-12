import pandas as pd





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
#
# # TODO: Add filter for age? Can only qualify for bafög if under 35?
