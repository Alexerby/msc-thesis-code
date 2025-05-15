"""
Module for merging statutory BAföG need components into the student dataset.

This module handles the parsing and integration of statutory tables that define
student financial need according to BAföG law, including:

- Basic need amounts (§13, §23)
- Housing allowances (§13)
- Health and long-term care insurance supplements (§13a)

The functions in this module perform time-aligned merges between survey data
and statutory rates, enabling the computation of total base need per student-year.
"""

import sys

import pandas as pd
import numpy as np


def merge_need_amounts(
    df: pd.DataFrame,
    students_df: pd.DataFrame,
    need_table: pd.DataFrame,
    insurance_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add statutory BAföG need components to the student DataFrame.

    This function:
    - Merges 'lives_at_home' from students_df using ['pid', 'syear']
    - Applies §13 (base need + housing) and §13a (insurance supplement)
    - Computes total need: base + housing + insurance

    Parameters
    ----------
    df : pd.DataFrame
        Minimal student-year DataFrame (must contain 'pid' and 'syear').
    students_df : pd.DataFrame
        Full student-level dataset with 'lives_at_home'.
    need_table : pd.DataFrame
        §13 base need table.
    insurance_table : pd.DataFrame
        §13a insurance supplement table.

    Returns
    -------
    pd.DataFrame
        Input DataFrame enriched with:
        - base_need
        - housing_allowance
        - insurance_supplement
        - total_base_need
    """
    required_cols = ["lives_at_home", "age"]
    if not all(col in students_df.columns for col in required_cols):
        print(students_df.columns)
        raise ValueError("`students_df` must contain 'lives_at_home' and 'age' columns.")



    # Parse statutory rules
    need = parse_need_table(need_table)
    ins = parse_insurance_table(insurance_table)

    # Convert year to datetime for validity matching
    df["syear_date"] = pd.to_datetime(df["syear"].astype(str) + "-01-01")

    # Merge statutory components
    df = merge_base_and_housing(df, need)
    df = merge_insurance_supplement(df, ins)

    # Calculate final need values
    df = calculate_need_components(df, students_df)

    # Drop intermediate variables
    return df.drop(
        columns=[
            "valid_from", "valid_from_ins", "syear_date",
            "base_need_parent", "base_need_away",
            "housing_with_parents", "housing_away", "lives_at_home",
            "kv_stat_mand", "pv_stat_mand"
        ],
        errors="ignore"
    )


def parse_need_table(need_table: pd.DataFrame) -> pd.DataFrame:
    """
    Rename and prepare the statutory 'needs' table for merging.

    This function standardizes the column names and parses the 'Valid from' column
    into a proper datetime format. It assumes the input table contains BAföG need 
    amounts per §23 and is ordered chronologically.

    Expected columns:
        - 'Valid from': effective date of the regulation
        - '§ 23 (1) 1': base need amount (living with parents)
        - '§ 23 (1) 2': base need amount (living away)
        - '§ 23 (1) 3': base need extra component (if applicable)

    Returns:
        A DataFrame with renamed and sorted columns:
            - valid_from
            - base_need_parent
            - base_need_away
            - base_need_extra
    """
    rename_map = {
        "Valid from": "valid_from",
        "§ 23 (1) 1": "base_need_parent",
        "§ 23 (1) 2": "base_need_away",
        "§ 23 (1) 3": "base_need_extra",
    }

    return (
        need_table
        .rename(columns=rename_map)
        .assign(valid_from=lambda d: pd.to_datetime(d["valid_from"]))
        .sort_values("valid_from")
    )


def parse_insurance_table(insurance_table: pd.DataFrame) -> pd.DataFrame:
    """
    Rename and prepare the statutory insurance supplement table for merging.

    This function standardizes column names and parses the 'Valid from' column
    into datetime format. It assumes the input contains insurance supplement 
    amounts defined in §13a for statutory health and long-term care.

    Expected columns:
        - 'Valid from': effective date of the regulation
        - '§ 13a (1) 1': statutory health insurance (Krankenversicherung, KV)
        - '§ 13a (1) 2': statutory long-term care insurance (Pflegeversicherung, PV)

    Returns:
        A DataFrame with:
            - valid_from_ins
            - kv_stat_mand
            - pv_stat_mand
        sorted chronologically by `valid_from_ins`.
    """
    rename_map = {
        "Valid from": "valid_from_ins",
        "§ 13a (1) 1": "kv_stat_mand",
        "§ 13a (1) 2": "pv_stat_mand",
    }

    return (
        insurance_table
        .rename(columns=rename_map)
        .assign(valid_from_ins=lambda d: pd.to_datetime(d["valid_from_ins"]))
        .sort_values("valid_from_ins")
    )


def merge_base_and_housing(df: pd.DataFrame, need: pd.DataFrame) -> pd.DataFrame:
    """
    Merge base need and housing allowance components onto student data.

    This function aligns the statutory need values from the §13 table 
    (base need and housing allowance) with each student's survey year (`syear_date`)
    using a backward merge.

    Assumes `df` contains a column `syear_date` (datetime) and that `need`
    contains statutory values keyed by 'Valid from'.

    Statutory columns:
        - '§ 13 (1) 1': base need (living with parents)
        - '§ 13 (1) 2': base need (living away from home)
        - '§ 13 (2) 1': housing allowance (with parents)
        - '§ 13 (2) 2': housing allowance (away from home)

    Returns:
        DataFrame with added columns:
            - base_need_parent
            - base_need_away
            - housing_with_parents
            - housing_away
    """
    need = need.rename(columns={
        "Valid from": "valid_from",
        "§ 13 (1) 1": "base_need_parent",
        "§ 13 (1) 2": "base_need_away",
        "§ 13 (2) 1": "housing_with_parents",
        "§ 13 (2) 2": "housing_away"
    })

    return pd.merge_asof(
        df.sort_values("syear_date"),
        need[[
            "valid_from", "base_need_parent", "base_need_away",
            "housing_with_parents", "housing_away"
        ]],
        left_on="syear_date",
        right_on="valid_from",
        direction="backward"
    )


def merge_insurance_supplement(df: pd.DataFrame, ins: pd.DataFrame) -> pd.DataFrame:
    #TODO: Make insurance supplement conditional on living_at_home
    # Assume no insurance if lives at home?
    """
    Merge statutory insurance supplements onto student data.

    Performs a backward `merge_asof` to align the applicable insurance
    supplement amounts based on the student's survey year (`syear_date`)
    and the "Valid from" dates in the §13a statutory table.

    Statutory columns:
        - '§13a (1) 1': mandatory health insurance (kv_stat_mand)
        - '§13a (1) 2': mandatory long-term care insurance (pv_stat_mand)

    Assumes:
        - `df` contains a datetime column `syear_date`
        - `ins` contains a 'Valid from' column and statutory amounts

    Returns:
        DataFrame with added columns:
            - kv_stat_mand (statutory health insurance supplement)
            - pv_stat_mand (statutory long-term care insurance supplement)
    """
    ins = ins.rename(columns={
        "Valid from": "valid_from_ins",
        "§13a (1) 1": "kv_stat_mand",
        "§13a (1) 2": "pv_stat_mand"
    })

    return pd.merge_asof(
        df.sort_values("syear_date"),
        ins[["valid_from_ins", "kv_stat_mand", "pv_stat_mand"]],
        left_on="syear_date",
        right_on="valid_from_ins",
        direction="backward",
        allow_exact_matches=True
    )




# def calculate_need_components(df: pd.DataFrame, students_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Compute the statutory BAföG need components for each student.
#
#     Adds the following columns to the DataFrame:
#       - base_need: based on whether the student lives at home
#       - housing_allowance: same conditional logic as base need
#       - insurance_supplement: sum of mandatory health (KV) and LTC (PV) contributions
#       - total_base_need: sum of all three components
#
#     Also zeroes out insurance supplement for students who are:
#       - under age 27
#       - and living at home (assumed to be family-insured)
#
#     Assumes the following columns are present:
#       - lives_at_home
#       - base_need_parent, base_need_away
#       - housing_with_parents, housing_away
#       - kv_stat_mand, pv_stat_mand
#       - age (from students_df, not necessarily in df)
#     
#     Returns:
#         DataFrame with new component columns added.
#     """
#
#     # Temporarily bring in ["age", "lives_at_home"] from students_df
#     df = df.merge(
#         students_df[["pid", "syear", "age", "lives_at_home"]],
#         on=["pid", "syear"],
#         how="left"
#     )
#     # Set base need depending on living situation
#     df["base_need"] = np.where(
#         df["lives_at_home"], df["base_need_parent"], df["base_need_away"]
#     )
#
#     # Set housing allowance similarly
#     df["housing_allowance"] = np.where(
#         df["lives_at_home"], df["housing_with_parents"], df["housing_away"]
#     )
#
#
#     # Assume family insurance for students under 27 who live at home
#     mask_family_insured = (df["lives_at_home"]) & (df["age"] < 27)
#     df["kv_stat_mand"] = np.where(mask_family_insured, 0, df["kv_stat_mand"])
#     df["pv_stat_mand"] = np.where(mask_family_insured, 0, df["pv_stat_mand"])
#
#     # Total insurance supplement
#     df["insurance_supplement"] = (
#         df["kv_stat_mand"].fillna(0) + df["pv_stat_mand"].fillna(0)
#     )
#
#     # Total base need
#     df["total_base_need"] = (
#         df["base_need"].fillna(0)
#         + df["housing_allowance"].fillna(0)
#         + df["insurance_supplement"].fillna(0)
#     )
#
#     # Drop age to avoid polluting the downstream pipeline
#     return df.drop(columns=["age", "lives_at_home"])


def calculate_need_components(df: pd.DataFrame, students_df) -> pd.DataFrame:
    """
    Compute the statutory BAföG need components for each student.

    Adds the following columns to the DataFrame:
      - base_need: based on whether the student lives at home
      - housing_allowance: same conditional logic as base need
      - insurance_supplement: sum of mandatory health (KV) and LTC (PV) contributions
      - total_base_need: sum of all three components

    Assumes the following columns are present:
      - lives_at_home
      - base_need_parent, base_need_away
      - housing_with_parents, housing_away
      - kv_stat_mand, pv_stat_mand

    Returns:
        DataFrame with new component columns added.
    """
    # Temporarily bring in ["age", "lives_at_home"] from students_df
    df = df.merge(
        students_df[["pid", "syear", "age", "lives_at_home"]],
        on=["pid", "syear"],
        how="left"
    )

    df["base_need"] = np.where(
        df["lives_at_home"], df["base_need_parent"], df["base_need_away"]
    )
    df["housing_allowance"] = np.where(
        df["lives_at_home"], df["housing_with_parents"], df["housing_away"]
    )
    df["insurance_supplement"] = (
        df["kv_stat_mand"].fillna(0) + df["pv_stat_mand"].fillna(0)
    )
    df["total_base_need"] = (
        df["base_need"].fillna(0)
        + df["housing_allowance"].fillna(0)
        + df["insurance_supplement"].fillna(0)
    )


    return df.drop(columns=["age", "lives_at_home"])
