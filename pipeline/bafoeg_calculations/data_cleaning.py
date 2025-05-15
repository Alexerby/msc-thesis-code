import pandas as pd

def clean_bafög_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize BAföG columns:
    - Remove invalid reported values (plc0168_h > total_base_need)
    - Create cleaned reported_bafög (capped and non-NaN)
    - Add boolean for theoretical take-up
    - Rename plc0167_h to a more descriptive name
    - Drop original plc0168_h column
    - Reorder columns for final output
    """

    # Clean reported amount
    df["reported_bafög"] = df["plc0168_h"].where(
        df["plc0168_h"] <= df["total_base_need"]
    ).fillna(0.0)

    # Rename dummy
    df = df.rename(columns={"plc0167_h": "received_bafög"})


    # Drop original reported column
    df = df.drop(columns=["plc0168_h"])

    # Reorder columns explicitly
    desired_order = [
        "pid", "syear", "base_need", "housing_allowance", "insurance_supplement",
        "total_base_need", "excess_income_stu", "excess_income_par",
        "received_bafög", "reported_bafög", "theoretical_bafög"
    ]
    # Add any remaining columns at the end
    remaining_cols = [col for col in df.columns if col not in desired_order]
    df = df[desired_order + remaining_cols]

    return df
