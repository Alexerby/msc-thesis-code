import pandas as pd
from tabulate import tabulate


def summarize_gross_income(
    df: pd.DataFrame,
    exclude_zero: bool = True,
    print_table: bool = True,
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
    print_table : bool
        Whether to print the summary using tabulate for pretty terminal output.

    Returns
    -------
    pd.DataFrame
        Summary statistics grouped by 'syear'
    """
    # Filter to valid income entries
    df = df[df["gross_annual_income"].notna()]
    income = df[["syear", "gross_annual_income", "gross_monthly_income", "kal2a02"]]

    # Optionally exclude zero income
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

    # Round income-related columns to integers
    round_cols = [
        "mean", "std", "min", "p5", "p25", "median",
        "p75", "p95", "max", "avg_gross_monthly_income"
    ]
    for col in round_cols:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").round(0).astype("Int64")

    # Round avg_kal2a02 to two decimal places
    summary["avg_kal2a02"] = summary["avg_kal2a02"].round(2)

    # Pretty print the table
    if print_table:
        print(tabulate(summary, headers="keys", tablefmt="fancy_grid", showindex=False))

    return summary


def summarize_excess_income(
    df: pd.DataFrame,
    exclude_zero: bool = True,
    print_table: bool = True,
) -> pd.DataFrame:
    """
    Create a summary table of excess income over survey years.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'syear' and 'excess_income'.
    exclude_zero : bool
        Whether to exclude rows with zero excess income.
    print_table : bool
        Whether to print the summary using tabulate.

    Returns
    -------
    pd.DataFrame
        Summary statistics grouped by 'syear'.
    """
    df = df[df["excess_income"].notna()]
    data = df[["syear", "excess_income"]]

    if exclude_zero:
        data = data[data["excess_income"] > 0]

    summary = data.groupby("syear").agg(
        n_obs=("excess_income", "count"),
        mean=("excess_income", "mean"),
        std=("excess_income", "std"),
        min=("excess_income", "min"),
        p5=("excess_income", lambda x: x.quantile(0.05)),
        p25=("excess_income", lambda x: x.quantile(0.25)),
        median=("excess_income", "median"),
        p75=("excess_income", lambda x: x.quantile(0.75)),
        p95=("excess_income", lambda x: x.quantile(0.95)),
        max=("excess_income", "max"),
    ).reset_index()

    # Round income-related columns to nearest integer
    round_cols = [
        "mean", "std", "min", "p5", "p25", "median",
        "p75", "p95", "max"
    ]
    for col in round_cols:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").round(0).astype("Int64")

    if print_table:
        print(tabulate(summary, headers="keys", tablefmt="fancy_grid", showindex=False))

    return summary
