import pandas as pd
from descriptives.helpers import load_data
from tabulate import tabulate


def merge_all_data() -> pd.DataFrame:
    """
    Load and merge all relevant datasets into a single DataFrame.
    """
    bc_df = load_data("bafoeg_calculations", from_parquet=True)
    stu_df = load_data("students", from_parquet=True)
    sib_joint_df = load_data("siblings_joint", from_parquet=True)
    parents_joint_df = load_data("parents_joint", from_parquet=True)

    # Prepare siblings data
    siblings_df = sib_joint_df[["student_pid", "syear", "any_sibling_bafog"]].drop_duplicates()
    bc_df = bc_df.merge(
        siblings_df,
        left_on=["pid", "syear"],
        right_on=["student_pid", "syear"],
        how="left"
    )

    # Merge parents
    bc_df = bc_df.merge(
        parents_joint_df,
        left_on=["pid", "syear"],
        right_on=["student_pid", "syear"],
        how="left"
    )

    # Merge student data
    main_df = bc_df.merge(stu_df, on=["pid", "syear"], how="left")
    return main_df


def get_only_non_takeup(main_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the dataset to only include non-take-up observations.
    """
    return main_df[main_df["non_take_up_obs"] == 1].copy()


def clean_and_prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Recode 'sex' to binary
    df["female"] = df["sex"].apply(lambda x: 1 if x == 2 else (0 if x == 1 else pd.NA))

    # Recode migration background to binary (any vs. none)
    df["has_migration_background"] = df["migback"].apply(lambda x: 1 if x in [2, 3] else 0 if x == 1 else pd.NA)

    # Filter invalid behavior scale values
    for col in ["plh0253", "plh0254", "plh0204_h"]:
        df.loc[(df[col] < 0) | (df[col] > 10), col] = pd.NA

    return df


def compute_summary(df: pd.DataFrame, display_vars: dict) -> pd.DataFrame:
    """
    Compute mean, min, max, and nobs (variable-wise) for selected variables.
    """
    summary_df = df[list(display_vars.keys())].rename(columns=display_vars)
    desc = summary_df.describe().T[["mean", "min", "max"]].round(2)
    return desc


def main():
    # Merge all data
    full_df = merge_all_data()

    # Prepare both samples
    full_df_clean = clean_and_prepare(full_df)
    ntu_df_clean = clean_and_prepare(get_only_non_takeup(full_df))


    # Define variables to summarize
    display_vars = {
        "theoretical_bafög": "Simulated BAföG (EUR)",
        "age": "Age",
        "female": "Female (%)",
        "has_partner": "Has partner (%)",
        "has_migration_background": "Migration background (%)",
        "lives_at_home": "Lives at home (%)",
        "any_sibling_bafog": "Sibling claimed BAföG (%)",
        "east_background": "East background (%)",
        "parent_high_edu": "Parents highly educated (%)",
        "plh0253": "Patience (0–10)",
        "plh0254": "Impulsiveness (0–10)",
        "plh0204_h": "Risk appetite (0–10)"
    }

    # Compute summaries
    desc_all = compute_summary(full_df_clean, display_vars)
    desc_ntu = compute_summary(ntu_df_clean, display_vars)

    # Combine side by side
    combined = desc_ntu.add_suffix(" (NTU)").join(desc_all.add_suffix(" (All)"))

    # Print table
    print(tabulate(combined, headers="keys", tablefmt="github"))


if __name__ == "__main__":
    main()
