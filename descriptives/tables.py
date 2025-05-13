import pandas as pd

def summarize_gross_income(
    df: pd.DataFrame,
    exclude_zero: bool = True
) -> pd.DataFrame:
    """
    Create a summary table of gross annual income and average working months
    over survey years.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'syear', 'gross_annual_income', and 'kal2a02'
    exclude_zero : bool
        Whether to exclude students with zero income from summary stats.

    Returns
    -------
    pd.DataFrame
        Summary statistics grouped by 'syear'
    """
    # Keep relevant columns
    income = df[["syear", "gross_annual_income", "gross_monthly_income", "kal2a02"]].dropna(subset=["gross_annual_income"])

    # Optionally exclude students with zero income
    if exclude_zero:
        income = income[income["gross_annual_income"] > 0]

    # Group and summarize
    summary = income.groupby("syear").agg(
        n_obs=("gross_annual_income", "count"),
        mean=("gross_annual_income", "mean"),
        std=("gross_annual_income", "std"),
        min=("gross_annual_income", "min"),
        p5=("gross_annual_income", lambda x: x.quantile(0.05)),
        p25=("gross_annual_income", lambda x: x.quantile(0.25)),
        median=("gross_annual_income", "median"),
        p75=("gross_annual_income", lambda x: x.quantile(0.75)),
        p95=("gross_annual_income", lambda x: x.quantile(0.95)),
        max=("gross_annual_income", "max"),
        avg_kal2a02=("kal2a02", "mean"),
        avg_gross_monthly_income=("gross_monthly_income", "mean"),
    ).reset_index()

    return summary
