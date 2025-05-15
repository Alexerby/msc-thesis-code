import pandas as pd
from . import sanity

from pipeline.soep_bundle import SOEPDataBundle

def merge_reported_bafög(
    df: pd.DataFrame,
    data: SOEPDataBundle
) -> pd.DataFrame:
    """
    Merge both the cleaned BAföG receipt indicator and the reported monthly amount
    into the main student dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Main DataFrame containing student-level data.
    data : SOEPDataBundle
        Bundle containing SOEP Core input DataFrames, including 'pl'.

    Returns
    -------
    pd.DataFrame
        The original dataset with 'plc0167_h' and 'plc0168_h' merged in.
    """
    df = merge_reported_bafög_receipt(df, data.pl)
    df = merge_reported_amounts_plc0168(df, data.pl)

    sanity.assert_no_inconsistent_bafög_amounts(df)
    return df


def merge_reported_bafög_receipt(
        df: pd.DataFrame,
        pl_df: pd.DataFrame) -> pd.DataFrame:
    #TODO: we have rows that are NaN - double check
    """
    Merge the cleaned BAföG receipt indicator (plc0167_h) into the main dataset.

    This indicator reflects whether the respondent reported receiving any 
    BAföG, scholarship, or vocational training assistance (binary: yes/no).

    Parameters:
    - df: Main DataFrame containing student-level data.
    - pl_df: The 'pl' dataset with raw BAföG receipt responses (plc0167_h).

    Returns:
    - pd.DataFrame: The input DataFrame with the cleaned 'plc0167_h' (receipt status) merged in.
    """
    out = df.copy()

    # Clean the binary receipt indicator before merging
    pl_clean = clean_plc0167_h(pl_df)

    # Select only the cleaned indicator for merging
    pl_clean = pl_clean[["pid", "syear", "plc0167_h"]]

    # Merge into the main dataset
    out = out.merge(pl_clean, on=["pid", "syear"], how="left")

    return out


def clean_plc0167_h(pl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and binarize the BAföG receipt indicator 'plc0167_h' so that:
      -  1 means “received”  → 1
      - -2 means “does not apply” (not receiving) → 0
      - all other codes (−1, −3…−8, NaN) are dropped

    Returns a DataFrame with only pid, syear, and cleaned plc0167_h.
    """
    pl = pl_df.copy()

    # Map only the codes we want, everything else → <NA>
    pl["plc0167_h"] = pl["plc0167_h"].map({1: 1, -2: 0})

    # Drop any rows where plc0167_h is missing (i.e. codes other than 1 or -2)
    pl = pl.dropna(subset=["plc0167_h"])

    # Ensure integer dtype
    pl["plc0167_h"] = pl["plc0167_h"].astype(int)

    return pd.DataFrame(pl[["pid", "syear", "plc0167_h"]])


def reconcile_received_with_reported(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wherever reported_bafög == 0, force received_bafög to 0.
    """
    out = df.copy()
    # any “false positive” received → overwrite to zero
    mask = out["reported_bafög"] == 0 # amount
    out.loc[mask, "received_bafög"] = 0 # 0/1
    return out


def clean_plc0168_h(pl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the reported monthly BAföG amount variable 'plc0168_h' by 
    setting invalid or non-informative codes to missing.

    Valid values:
    - Any positive number: Respondent reported a monthly BAföG/stipend amount in EUR.
    
    Treated as missing (set to pd.NA):
    - -1 : No answer / refused
    - -2 : Does not apply (e.g., respondent did not receive BAföG)
    - -3 to -8 : Implausible, invalid, or not part of the questionnaire

    Parameters:
    - pl_df (pd.DataFrame): The 'pl' dataset containing raw income responses.

    Returns:
    - pd.DataFrame: A copy of the dataset with 'plc0168_h' cleaned.
    """
    pl = pl_df.copy()

    # Replace invalid or non-positive values with missing
    pl["plc0168_h"] = pl["plc0168_h"].where(pl["plc0168_h"] > 0, pd.NA)

    return pl


def merge_reported_amounts_plc0168(
        df: pd.DataFrame,
        pl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge cleaned reported monthly BAföG/stipend amounts (plc0168_h) into the main dataset.

    Parameters:
    - df: Main DataFrame containing student-level data.
    - pl_df: The 'pl' dataset containing raw reported BAföG amount responses.

    Returns:
    - pd.DataFrame: The original DataFrame with 'plc0168_h' merged in (cleaned).
    """
    out = df.copy()

    # Clean the monthly amount before merging
    pl_clean = clean_plc0168_h(pl_df)

    # Select only relevant columns
    pl_clean = pl_clean[["pid", "syear", "plc0168_h"]]

    # Merge into main dataset
    out = out.merge(pl_clean, on=["pid", "syear"], how="left")

    return out
