import pandas as pd

def winsorize_upper_tail(df: pd.DataFrame, upper_tail: float = 0.05) -> pd.DataFrame:
    """
    Removes the top `upper_tail` fraction of gross_annual_income values per year (syear).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'gross_annual_income' and 'syear'
    upper_tail : float, optional (default=0.05)
        Fraction of upper-end values to remove (e.g., 0.05 removes the top 5%)

    Returns
    -------
    pd.DataFrame
        DataFrame with upper outliers removed
    """
    def remove_upper_tail(group):
        upper_cutoff = group["gross_annual_income"].quantile(1 - upper_tail)
        return group[group["gross_annual_income"] <= upper_cutoff]

    return df.groupby("syear", group_keys=False).apply(remove_upper_tail)
