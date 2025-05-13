import pandas as pd


from .need_components import merge_need_amounts 
from .reported_amount import merge_reported_bafög

def create_dataframe(df: pd.DataFrame, 
                     need_table: pd.DataFrame, 
                     insurance_table: pd.DataFrame,
                     pl_df: pd.DataFrame
                     ) -> pd.DataFrame:
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

    out = df.copy()

    out = merge_need_amounts(out, need_table, insurance_table)

    out = merge_reported_bafög(out, pl_df)

    return out

