import pandas as pd

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



