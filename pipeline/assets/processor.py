import pandas as pd
import numpy as np

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle

def create_dataframe(
    df: pd.DataFrame,  # minimal frame: just pid + syear
    *,
    students_df: pd.DataFrame,
    policy: PolicyTableBundle,
    data: SOEPDataBundle,
) -> pd.DataFrame:
    out = df.copy()

    # Merge assets
    out = merge_assets(out, data.pwealth)

    return out

def merge_assets(
    df: pd.DataFrame,
    pwealth_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge average imputed asset values from SOEP wealth module
    onto the student-level DataFrame. Includes:
    - Financial assets
    - Other real estate (net share value)
    - Business assets
    - Private insurances
    - Vehicles
    - Tangible assets
    - Debts
    """
    df = df.copy()

    print(pwealth_df["syear"].value_counts())

    base_cols = ["pid", "syear"]
    asset_groups = {
        "financial_assets": ["f0100a", "f0100b", "f0100c", "f0100d", "f0100e"],
        "real_estate": ["e0111a", "e0111b", "e0111c", "e0111d", "e0111e"],
        "business_assets": ["b0100a", "b0100b", "b0100c", "b0100d", "b0100e"],
        "private_insurances": ["i0100a", "i0100b", "i0100c", "i0100d", "i0100e"],
        "vehicles": ["v0100a", "v0100b", "v0100c", "v0100d", "v0100e"],
        "tangible_assets": ["t0100a", "t0100b", "t0100c", "t0100d", "t0100e"],
        "debts": ["w0011a", "w0011b", "w0011c", "w0011d", "w0011e"]
    }

    # Slice required columns
    cols = base_cols + [col for group in asset_groups.values() for col in group]
    pwealth = pwealth_df[cols].copy()

    # Clean invalid values
    invalid_codes = {-1, -3, -4, -5, -6, -7, -8}
    for cols in asset_groups.values():
        pwealth[cols] = pwealth[cols].where(~pwealth[cols].isin(invalid_codes), np.nan)

    # Compute average per asset category
    for key, cols in asset_groups.items():
        pwealth[key] = pwealth[cols].mean(axis=1)

    # Fill missing asset values with 0 (except debts, which are already negative)
    for key in asset_groups:
        pwealth[key] = pwealth[key].fillna(0)

    # Compute total assets
    pwealth["total_assets"] = (
        pwealth["financial_assets"]
        + pwealth["real_estate"]
        + pwealth["business_assets"]
        + pwealth["private_insurances"]
        + pwealth["vehicles"]
        + pwealth["tangible_assets"]
        - pwealth["debts"]  # debts reduce total
    )

    # Merge back onto main DataFrame
    asset_vars = base_cols + list(asset_groups.keys()) + ["total_assets"]
    out = df.merge(pwealth[asset_vars], on=["pid", "syear"], how="left")

    return out
