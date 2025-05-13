import pandas as pd

from pipeline.common.data_cleaning import (
        winsorize_upper_tail
        )

def merge_income(
    df: pd.DataFrame,
    pkal_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge gross income from employment into the main DataFrame using the total
    annual salary from the pkal dataset (estimated as average monthly income ×
    number of income months).

    Treatment:
    - Invalid codes (-1 to -8 except -2) → treated as NaN
    - -2 in income or months → set to 0 (explicitly not working)
    - Allows fractional and >12 months (e.g. 5.5, 13) as valid
    - Final annual income winsorized at 97.5th percentile per year

    Parameters
    ----------
    df : pd.DataFrame
        Main DataFrame with 'pid' and 'syear'.
    pkal_df : pd.DataFrame
        SOEP pkal DataFrame with 'kal2a03_h' and 'kal2a02'

    Returns
    -------
    pd.DataFrame
        DataFrame with:
        - 'gross_monthly_income': kal2a03_h (cleaned)
        - 'gross_annual_income': kal2a03_h × kal2a02 (winsorized)
    """
    pkal = pkal_df.copy()

    # Clean invalid codes
    invalid_codes = [-1, -3, -4, -5, -6, -7, -8]
    pkal["kal2a03_h"] = pkal["kal2a03_h"].replace(invalid_codes, pd.NA)
    pkal["kal2a02"] = pkal["kal2a02"].replace(invalid_codes, pd.NA)

    # Replace -2 ("not working") with 0
    pkal.loc[pkal["kal2a03_h"] == -2, "kal2a03_h"] = 0
    pkal.loc[pkal["kal2a02"] == -2, "kal2a02"] = 0

    # Compute monthly and annual gross income
    pkal["gross_monthly_income"] = pkal["kal2a03_h"]
    pkal["gross_annual_income"] = pkal["kal2a03_h"] * pkal["kal2a02"]

    # Drop rows with missing result
    pkal = pkal.dropna(subset=["gross_annual_income", "gross_monthly_income"])

    # Merge with student DataFrame
    out = df.merge(
        pkal[["pid", "syear", "gross_monthly_income", "kal2a02", "gross_annual_income"]],
        on=["pid", "syear"],
        how="inner"
    )

    # Apply winsorization at upper 2.5% tail
    out = winsorize_upper_tail(out, 0.05)

    return out


