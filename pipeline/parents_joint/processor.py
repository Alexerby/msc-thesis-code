import pandas as pd
import numpy as np

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle
from pipeline.common.processor_deductions import apply_entity_allowances
from pipeline.common.filters import drop_zero_rows

def create_dataframe(
    parents_df: pd.DataFrame,
    data: SOEPDataBundle, 
    policy: PolicyTableBundle,
    siblings_joint_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Builds a parent-level DataFrame with sociodemographic enrichment.
    """
    return (
        base_view(parents_df, policy)
        .pipe(merge_sibling_deduction, siblings_joint_df=siblings_joint_df)
        .pipe(subtract_sibling_deduction)
        .pipe(apply_additional_allowance)
        .pipe(drop_zero_rows, "joint_income")
    )


def apply_additional_allowance(df: pd.DataFrame) -> pd.DataFrame:
    # Treat NaNs as zero and only count siblings with positive sib_deduction
    num_income_relevant_siblings = df["num_siblings"].fillna(0).where(df["sib_deduction"] > 0, 0)

    return df.assign(
        additional_allowance=lambda d: d["joint_income_less_ba_and_sib"] * (
            0.50 + 0.05 * num_income_relevant_siblings
        ),
        excess_income=lambda d: np.maximum(
            d["joint_income_less_ba_and_sib"] - d["additional_allowance"], 0
        )
    )

def subtract_sibling_deduction(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        joint_income_less_ba_and_sib=lambda d: np.maximum(
            d["joint_income_less_ba"] - d["sib_deduction"], 0
        )
    )


def merge_sibling_deduction(df: pd.DataFrame, siblings_joint_df: pd.DataFrame) -> pd.DataFrame:
    merged = df.merge(
        siblings_joint_df[["student_pid", "syear", "joint_income", "num_siblings"]]
            .rename(columns={"joint_income": "sib_deduction"}),
        on=["student_pid", "syear"],
        how="left"
    )
    # fill NaNs, then cast to the exact dtypes you want
    merged["sib_deduction"] = merged["sib_deduction"].fillna(0).astype(float)
    merged["num_siblings"]  = merged["num_siblings"].fillna(0).astype(int)
    return merged


def base_view(parents_df: pd.DataFrame, policy: PolicyTableBundle) -> pd.DataFrame:
    """
    Constructs the base parental income view by aggregating net monthly income
    from contributing parents and counting only those who contribute financially.
    Now also computes average, min, and max parental education per student.
    """
    contributing = parents_df[parents_df["net_monthly_income"] > 0]

    # Convert ISCED to numeric (safe, in case of stringy merges)
    contributing["pgisced11"] = pd.to_numeric(contributing["pgisced11"], errors="coerce")

    base = (
        contributing
        .groupby(["student_pid", "syear"])
        .agg(
            joint_income=("net_monthly_income", "sum"),
            num_parents=("pid", "count"),
            # You can compute any/all of these:
            isced_mean=("pgisced11", "mean"),
            # Optionally: most frequent (mode)
            # isced_mode=('pgisced11', lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA),
        )
        .reset_index()
    )

    # Apply single-parent allowance
    single = base.query("num_parents == 1").pipe(
        apply_entity_allowances,
        allowance_table=policy.allowance_25.rename(columns={
            "§ 25 (1) 1": "allowance_joint",
            "§ 25 (1) 2": "allowance_single"
        }),
        entity_keys={"parent": "allowance_single"},
        entity_counts={},
        base_entity="parent",
        income_col="joint_income",
        output_col="joint_income_less_ba"
    )

    # Apply two-parent allowance
    joint = base.query("num_parents == 2").pipe(
        apply_entity_allowances,
        allowance_table=policy.allowance_25.rename(columns={
            "§ 25 (1) 1": "allowance_joint",
            "§ 25 (1) 2": "allowance_single"
        }),
        entity_keys={"parent": "allowance_joint"},
        entity_counts={},
        base_entity="parent",
        income_col="joint_income",
        output_col="joint_income_less_ba"
    )

    return pd.concat([single, joint]).sort_values(["student_pid", "syear"]).reset_index(drop=True)
