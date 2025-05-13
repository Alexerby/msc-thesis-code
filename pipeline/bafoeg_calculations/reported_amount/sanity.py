import pandas as pd

def check_inconsistent_bafög_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify cases where a BAföG amount is reported (plc0168_h not missing),
    but the respondent explicitly stated they do NOT receive BAföG (plc0167_h == -2).

    Parameters:
    - df: DataFrame with 'plc0167_h' and 'plc0168_h' columns.

    Returns:
    - DataFrame of inconsistent rows (can be empty).
    """
    return pd.DataFrame(df[
        (df["plc0167_h"] == -2) & (df["plc0168_h"].notna())
    ])


def assert_no_inconsistent_bafög_amounts(df: pd.DataFrame) -> None:
    """
    Assert that no one reports a BAföG amount while also stating 
    they do not receive BAföG.

    Raises:
    - ValueError if any such inconsistencies are found.
    """
    invalid = check_inconsistent_bafög_amounts(df)
    if not invalid.empty:
        raise ValueError(
            f"{len(invalid)} inconsistent BAföG entries found: "
            "Amount is reported despite plc0167_h == -2."
        )
