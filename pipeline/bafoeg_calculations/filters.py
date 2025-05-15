import pandas as pd

def drop_reported_bafog_inconsistencies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out any rows where received_bafög = 1 but reported_bafög = 0.
    """
    mask = (df['received_bafög'] == 1) & (df['reported_bafög'] == 0)
    # keep only the rows that are *not* inconsistent
    return df.loc[~mask].reset_index(drop=True)


def clamp_small_theoretical_awards(
    df: pd.DataFrame,
    *,
    award_col: str = "theoretical_bafög",
    threshold: float = 50.0
) -> pd.DataFrame:
    """
    Zero‐out any theoretical BAföG awards that are >0 but <= threshold,
    to avoid tiny spurious model artifacts.
    """
    mask = (df[award_col] > 0) & (df[award_col] <= threshold)
    df.loc[mask, award_col] = 0
    return df


