import pandas as pd

from .need_components import merge_need_amounts 

def create_dataframe(df: pd.DataFrame, need_table: pd.DataFrame, insurance_table: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame containing calculated BAföG need components.

    Parameters
    ----------
    df : pd.DataFrame
        Input student-level DataFrame with at least ['pid', 'syear', 'lives_at_home'].
    need_table : pd.DataFrame
        Statutory needs table (e.g. from §13 BAföG).
    insurance_table : pd.DataFrame
        Statutory insurance supplement table (e.g. from §13a BAföG).

    Returns
    -------
    pd.DataFrame
        DataFrame enriched with BAföG need components.
    """
    df_with_needs = merge_need_amounts(df, need_table, insurance_table)
    return df_with_needs

