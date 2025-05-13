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
    Clean the BAföG receipt indicator variable 'plc0167_h' by retaining only valid, 
    interpretable responses and setting all others to missing.

    Valid values retained:
    - 1 : Respondent explicitly reported receiving BAföG, a scholarship, 
          or vocational training support.
    - -2: Respondent was presented with the question and explicitly did 
          not check the box for BAföG-related income — i.e., they do not receive it.

    As documented (see SOEP Survey Paper 1256, p. 75), -2 represents a valid 
    "does not apply" response resulting from the respondent's answer, 
    not a structural survey skip.

    All other codes (e.g., -1, -3 to -8) are treated as invalid or ambiguous:
    - -1 : No answer / refused
    - -3 to -8 : Implausible or structurally missing due to questionnaire routing or versioning

    These are set to missing (NA) to avoid bias or misclassification.

    Parameters:
    - pl_df (pd.DataFrame): The 'pl' dataset containing raw person-level income variables.

    Returns:
    - pd.DataFrame: A copy of the dataset with 'plc0167_h' cleaned and non-informative
      values replaced with pd.NA.
    """
    pl = pl_df.copy()

    # Retain only valid responses; set others to missing (NA)
    valid_values = [1, -2]
    pl["plc0167_h"] = pl["plc0167_h"].where(pl["plc0167_h"].isin(valid_values), pd.NA)

    return pl


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
