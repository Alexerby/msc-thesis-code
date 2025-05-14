import pandas as pd

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
