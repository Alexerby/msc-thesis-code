import pandas as pd
from pandas.core.api import DataFrame

from pipeline.common.processor_sociodemographics import add_age


from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle


from pipeline.common.processor_income import merge_income
from pipeline.common.processor_deductions import (
        apply_table_deduction, 
        apply_tax, 
        apply_entity_allowances
)

from services.tax import TaxService



def create_dataframe(
        students_df: pd.DataFrame,
        data: SOEPDataBundle, 
        policy: PolicyTableBundle,
        ) -> pd.DataFrame:
    """
    Builds a parent-level DataFrame with sociodemographic enrichment.

    Parameters
    ----------
    data : SOEPDataBundle
        Bundle of relevant SOEP Core DataFrames.

    Returns
    -------
    pd.DataFrame
        Enriched student-level dataset
    """

    return (
        base_view(students_df) 
        .pipe(add_age, ppath_df=data.ppath)
        .pipe(merge_education_level, data.pgen)
        .pipe(merge_income, pkal_df=data.pkal)

        .pipe(
            apply_table_deduction,
            table=policy.werbung,
            merge_left_on="syear",
            merge_right_on="Year",
            deduction_column="werbungskostenpauschale",
            input_col="gross_annual_income",
            output_col="inc_w",
            method="max_zero",
            deduction_type="amount",
            drop_deduction_column=True
        )

        # 3. Apply Sozialversicherungs-Pauschale (year-varying rates, forward filled, with cap)
        .pipe(
            apply_table_deduction,
            table=policy.social_insurance,
            merge_left_on="syear",
            merge_right_on="Year",
            deduction_column="Rate",
            input_col="inc_w",
            output_col="inc_si",
            method="default",
            deduction_type="rate",
            forward_fill=True,
            cap=17_200,
            drop_deduction_column=True
        )

        # 4. Apply taxes 
        .pipe(
            apply_tax,
            tax_service=TaxService("inc_si"), 
            input_col="inc_si", 
            output_col="inc_net"
        )

        # 4.5. Compute monthly net income for deduction logic
        .assign(net_monthly_income = lambda d: d["inc_net"] / 12)
    )





def base_view(students_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates one row per parent per student.
    - `pid` is the parent's PID (main subject of each row),
    - `student_pid` refers to the associated student,
    - `syear` is the survey year.
    """

    # Keep relevant columns
    df = students_df[["pid", "syear", "fnr", "mnr"]].copy()

    # Replace invalid values
    df[["fnr", "mnr"]] = df[["fnr", "mnr"]].replace(-1, pd.NA)

    # Reshape to long format: one row per parent
    parents_long = df.melt(
        id_vars=["pid", "syear"],
        value_vars=["fnr", "mnr"],
        var_name="parent_role",   # optional: could be used later to distinguish mother/father
        value_name="parent_pid"
    )

    # Drop missing parent rows
    parents_long = parents_long.dropna(subset=["parent_pid"])

    # Convert to appropriate types
    parents_long["parent_pid"] = parents_long["parent_pid"].astype(int)

    # Rename for clarity and reorder
    parents_long = parents_long.rename(columns={
        "pid": "student_pid",
        "parent_pid": "pid"  # now pid refers to the parent
    })

    base_cols = ["pid", "syear", "student_pid", "parent_role"]

    return pd.DataFrame(parents_long[base_cols].reset_index(drop=True)) 



def harmonize_plg0014(parents_df: pd.DataFrame, pl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonizes the SOEP plg0014_* variables (education coding) into a single column 'plg0014_harmonized'.
    Keeps SOEP codes (except sets any negative value to NA).
    Left merges onto parents_df by 'pid'.
    """
    edu_cols = ["plg0014_v5", "plg0014_v6", "plg0014_v7"]
    
    # Copy to avoid modifying original
    pl_df = pl_df.copy()
    
    # Harmonize: take the first non-missing value in order
    pl_df["plg0014_harmonized"] = pl_df[edu_cols].bfill(axis=1).iloc[:, 0]
    
    # Set all negative values to pd.NA
    pl_df.loc[pl_df["plg0014_harmonized"] < 0, "plg0014_harmonized"] = pd.NA
    
    # Only keep pid and new column, drop duplicates
    pl_df = pl_df[["pid", "plg0014_harmonized"]].drop_duplicates(subset="pid")
    
    # Left merge onto parents_df by pid
    out = parents_df.merge(pl_df, on="pid", how="left")
    return out


def merge_education_level(parents_df: pd.DataFrame, pgen_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge ISCED-2011 education level (pgisced11) from pgen onto parents_df by pid, forward-filling by person.
    All negative or invalid codes are set to NA.
    Keeps column name as 'pgisced11'.
    """
    # Copy and clean
    pgen = pgen_df[["pid", "syear", "pgisced11"]].copy()
    pgen.loc[pgen["pgisced11"] < 0, "pgisced11"] = pd.NA

    # Sort and forward fill by person over time
    pgen = pgen.sort_values(["pid", "syear"])
    pgen["pgisced11"] = pgen.groupby("pid")["pgisced11"].ffill()

    # Keep only the pid, syear, and the forward-filled value
    pgen = pgen[["pid", "syear", "pgisced11"]].drop_duplicates()

    # Merge onto parents_df by pid and syear
    out = parents_df.merge(pgen, on=["pid", "syear"], how="left")
    return out
