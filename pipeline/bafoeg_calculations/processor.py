import pandas as pd
import numpy as np
from pandas.core.api import DataFrame



from .need_components import merge_need_amounts 
from .reported_amount import merge_reported_bafög
from .eligibility_conditions import is_ineligible
from .filters import clamp_small_theoretical_awards
from .data_cleaning import clean_bafög_columns

from pipeline.common.others import add_weight_col
from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle


def create_dataframe(
    df: pd.DataFrame,
    *,
    students_df: pd.DataFrame,
    parents_joint_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    policy: PolicyTableBundle,
    data: SOEPDataBundle,
) -> pd.DataFrame:
    """
    Build the BAföG pipeline in five clear steps, then reorder columns so that
    all excess‐income fields follow total_base_need, and asset excess appears
    alongside student/parent excess before reported/theoretical outputs.
    """
    out = df.copy()
    out = _merge_needs(out, students_df, policy)
    out = _merge_all_excesses(out, students_df, parents_joint_df, assets_df)
    out = merge_reported_bafög(out, data)
    

    out = _compute_theoretical_bafög(out, students_df, data.pl, data.pgen)
    out = clamp_small_theoretical_awards(out, threshold=50)
    out["theoretical_eligibility"] = (out["theoretical_bafög"] > 0).astype(int)


    # drop any rows where received=1 but reported=0
    # out = drop_reported_bafog_inconsistencies(out)
    out = clean_bafög_columns(out)
    # out = drop_zero_excess_income_par(out)

    out = add_weight_col(out, phrf_df=data.phrf)
    out = merge_ffill(out, data.pl, "plh0253")
    out = merge_ffill(out, data.pl, "plh0254")
    out = merge_ffill(out, data.pl, "plh0204_h")



    desired_order = [
        # identifying keys
        "pid", "syear",
        # need components
        "base_need", "housing_allowance", "insurance_supplement", "total_base_need",
        # all excess‐income in logical order
        "excess_income_stu", "excess_income_par", "excess_income_assets",
        # reported BAföG
        "received_bafög", "reported_bafög",
        # theoretical outputs
        "theoretical_bafög", "theoretical_eligibility",
        # any remaining columns...
    ]
    # Append any other columns that may follow
    rest = [c for c in out.columns if c not in desired_order]
    out = out[desired_order + rest]
    


    return pd.DataFrame(out)


def _merge_needs(
    df: pd.DataFrame,
    students_df: pd.DataFrame,
    policy: PolicyTableBundle
) -> pd.DataFrame:
    """Step 1: statutory base‐need + insurance allowances."""
    return merge_need_amounts(df, students_df, policy.needs, policy.insurance)


def _merge_all_excesses(
    df: pd.DataFrame,
    students_df: pd.DataFrame,
    parents_joint_df: pd.DataFrame,
    assets_df: pd.DataFrame
) -> pd.DataFrame:
    """Step 2: bring in student, parental, and asset excess incomes."""
    out = merge_student_excess_income(df, students_df)
    out = merge_parental_excess_income(out, parents_joint_df)
    out = merge_excess_assets(out, assets_df)

    # ensure zeros instead of NaN
    for col in ("excess_income_stu", "excess_income_par", "excess_income_assets"):
        out[col] = out.get(col, 0).fillna(0)

    return out


def _compute_theoretical_bafög(
    df: pd.DataFrame,
    students_df: pd.DataFrame,
    pl_df: pd.DataFrame,
    pgen_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Step 4: apply ineligibility, then
      theoretical = max(total_base_need – sum_of_excesses, 0)
    """
    out = df.copy()
    ineligible = is_ineligible(out, students_df, pl_df, pgen_df)
    total_excess = (
        out["excess_income_stu"]
      + out["excess_income_par"]
      + out["excess_income_assets"]
    )

    out["theoretical_bafög"] = np.where(
        ineligible,
        0,
        np.maximum(out["total_base_need"] - total_excess, 0)
    )
    return out


def merge_excess_assets(
    df: pd.DataFrame,
    assets_df: pd.DataFrame,
    asset_col: str = "excess_assets",
    output_col: str = "excess_income_assets"
) -> pd.DataFrame:
    """
    Merge in the precomputed asset‐based excess and rename it consistently.
    """
    if asset_col not in assets_df.columns:
        raise ValueError(f"`assets_df` must contain `{asset_col}`")

    trimmed = (
        assets_df[["pid", "syear", asset_col]]
        .rename(columns={asset_col: output_col})
    )

    out = df.merge(
        trimmed,
        on=["pid", "syear"],
        how="left",
        validate="one_to_one"
    )
    out[output_col] = out[output_col].fillna(0)
    return out


def merge_student_excess_income(
    df: pd.DataFrame,
    students_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge student-level excess income and rename to 'excess_income_stu'.
    """
    if "excess_income" not in students_df.columns:
        raise ValueError("`students_df` must contain 'excess_income'")

    return df.merge(
        students_df[["pid", "syear", "excess_income"]]
                   .rename(columns={"excess_income": "excess_income_stu"}),
        on=["pid", "syear"],
        how="left",
        validate="one_to_one"
    )


def merge_parental_excess_income(
    df: pd.DataFrame,
    parents_joint_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge parental excess income and rename to 'excess_income_par'.
    Also add 'ind_funded' dummy: 1 if joint_income < 0, else 0.
    """
    # Prepare the parental info with dummy
    parents = (
        parents_joint_df
        .rename(columns={
            "student_pid": "pid",
            "excess_income": "excess_income_par"
        })
        .loc[:, ["pid", "syear", "excess_income_par", "joint_income"]]
        .copy()
    )
    parents["ind_funded"] = (parents["joint_income"] < 0).astype(int)

    # Only keep columns needed for the merge
    parents = parents.loc[:, ["pid", "syear", "excess_income_par", "ind_funded"]]

    # Merge into left dataframe
    return df.merge(
        parents,
        on=["pid", "syear"],
        how="inner",
        validate="one_to_one"
    )

def merge_ffill(
    df: pd.DataFrame, 
    pl_df: pd.DataFrame, 
    var_name: str
) -> pd.DataFrame:
    # Only keep needed columns from pl_df to avoid accidental duplicate columns
    pl_df = pl_df[["pid", "syear", var_name]]

    out = df.copy()
    out = out.merge(pl_df, on=["pid", "syear"], how="left")
    
    # Sort by pid and year before forward filling
    out = out.sort_values(['pid', 'syear'])
    out[var_name] = out.groupby('pid')[var_name].ffill()
    
    return out
