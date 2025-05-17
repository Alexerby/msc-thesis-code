import pandas as pd 

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle

from pipeline.common.filters import filter_age, filter_students
from pipeline.common.processor_income import merge_income
from pipeline.common.processor_sociodemographics import add_age

from pipeline.common.processor_deductions import (
        apply_table_deduction, 
        apply_tax, 
        apply_entity_allowances
)




def create_dataframe(
    students_df: pd.DataFrame,
    data: SOEPDataBundle, 
    policy: PolicyTableBundle
) -> pd.DataFrame:
    """
    Builds a sibling-level DataFrame with sociodemographic enrichment using a pipeline style.
    """
    return (
        base_view(students_df)
        .pipe(merge_taken_bafog, pl_df = data.pl)
        .pipe(add_age, ppath_df=data.ppath)
        .pipe(filter_age, col_name="age")
        .pipe(filter_students, pl_df=data.pl, negate=True)  # non-student siblings
        .pipe(merge_income, pkal_df=data.pkal)

        # Step 1: Convert to monthly net
        .assign(net_inc_mo=lambda d: d["gross_annual_income"] / 12)

        # Step 2: Apply Tz 21.1.32 BAföGVwV
        .assign(net_inc_adj=lambda d: (d["net_inc_mo"] - 140).clip(lower=0))

        # Step 3: Apply § 23 (3) 2 allowance to monthly income
        .pipe(
            apply_entity_allowances,
            allowance_table=policy.allowance_25.rename(columns={
                "§ 25 (3) 2": "allowance_sibling"
            }),
            entity_keys={"sibling": "allowance_sibling"},
            entity_counts={},
            base_entity="sibling",
            income_col="net_inc_adj",
            output_col="excess_income",
            subtract_income_from_allowance=True
        )
    )


def base_view(students_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a sibling-level DataFrame by unpacking sibling_pids from the student-level data.

    Parameters
    ----------
    students_df : pd.DataFrame
        Student-level DataFrame with 'sibling_pids', 'pid', and 'syear'.

    Returns
    -------
    pd.DataFrame
        DataFrame with one row per sibling relationship.
    """
    # Explode the sibling_pids list so each sibling becomes a separate row
    siblings_df = students_df.explode('sibling_pids').copy()

    # Rename columns for clarity
    siblings_df = siblings_df.rename(columns={
        'pid': 'student_pid',
        'sibling_pids': 'pid'
    })

    # Drop rows with no sibling
    siblings_df = siblings_df.dropna(subset=['pid'])

    # Ensure correct type
    siblings_df['pid'] = siblings_df['pid'].astype(int)

    base_cols = ["pid", "syear", "student_pid"]

    return pd.DataFrame(siblings_df[base_cols])

def merge_taken_bafog(siblings_df: pd.DataFrame, pl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge siblings' 'has taken BAföG' status (plc0167_h) into the sibling-level DataFrame.
    Takes the last available value up to and including syear.
    """
    # Clean and prepare
    pl = pl_df[["pid", "syear", "plc0167_h"]].copy()
    # Set missing/negative to NA
    pl.loc[pl["plc0167_h"] < 0, "plc0167_h"] = pd.NA

    # For each (pid, syear), forward-fill within pid, then take the value for the year <= syear
    pl = pl.sort_values(["pid", "syear"])
    # Use groupby+ffill to fill missing over time
    pl["plc0167_h"] = pl.groupby("pid")["plc0167_h"].ffill()

    # For each sibling in the DataFrame, get their plc0167_h as of their syear
    siblings_with_bafog = siblings_df.merge(
        pl, on=["pid", "syear"], how="left"
    )

    # If you want to know if sibling EVER took BAföG (across ALL years), you can do:
    # Merge max(plc0167_h) over all years per pid
    has_ever_taken_bafog = pl.groupby("pid")["plc0167_h"].max().reset_index()
    siblings_with_bafog = siblings_df.merge(has_ever_taken_bafog, on="pid", how="left")

    return siblings_with_bafog
