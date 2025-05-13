
import pandas as pd
import numpy as np
from typing import Literal, Callable
from services.tax import TaxService

def apply_flat_rate_deduction(
    df: pd.DataFrame,
    input_col: str,
    output_col: str,
    rate: float,
) -> pd.DataFrame:
    """
    Applies a flat-rate percentage deduction to a column.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    input_col : str
        Column to apply the deduction to.
    output_col : str
        Name of the output column.
    rate : float
        Deduction rate (e.g. 0.223 for 22.3%)

    Returns
    -------
    pd.DataFrame
        DataFrame with new column added.
    """
    out = df.copy()
    out[output_col] = out[input_col] * (1 - rate)
    return out


def apply_table_deduction(
    df: pd.DataFrame,
    table: pd.DataFrame,
    *,
    merge_left_on: str = "syear",
    merge_right_on: str = "Year",
    deduction_column: str,
    input_col: str,
    output_col: str,
    method: Literal["default", "max_zero"] = "default",
    deduction_type: Literal["amount", "rate"] = "amount",
    drop_deduction_column: bool = True,
    forward_fill: bool = False,
    cap: float | None = None
) -> pd.DataFrame:
    """
    Merge a lookup table and apply either a fixed-amount or rate-based deduction.
    """
    if forward_fill:
        min_year = min(table[merge_right_on].min(), df[merge_left_on].min())
        max_year = df[merge_left_on].max()
        all_years = pd.DataFrame({merge_right_on: range(min_year, max_year + 1)})
        table = (
            all_years.merge(table[[merge_right_on, deduction_column]],
                            on=merge_right_on,
                            how="left")
            .sort_values(merge_right_on)
            .ffill()
        )

    merged = df.merge(table[[merge_right_on, deduction_column]],
                      left_on=merge_left_on,
                      right_on=merge_right_on,
                      how="left")

    if merge_right_on != merge_left_on:
        merged = merged.drop(columns=[merge_right_on])

    merged[input_col] = pd.to_numeric(merged[input_col], errors="coerce")
    merged[deduction_column] = pd.to_numeric(merged[deduction_column], errors="coerce")

    if deduction_type == "rate":
        deduction = merged[input_col] * merged[deduction_column]
    else:
        deduction = merged[deduction_column]

    if cap is not None:
        deduction = np.minimum(deduction, cap)

    if method == "max_zero":
        merged[output_col] = np.maximum(merged[input_col] - deduction, 0)
    else:
        merged[output_col] = merged[input_col] - deduction

    if drop_deduction_column:
        return merged.drop(columns=[deduction_column])

    return merged


def apply_tax(
    df: pd.DataFrame,
    tax_service: TaxService,
    *,
    input_col: str,
    output_col: str = "net_annual_income"
) -> pd.DataFrame:
    """
    Apply a tax service that returns a Series of tax components and compute net income.
    """
    out = df.copy()
    tax_components = out.apply(tax_service.compute_for_row, axis=1, result_type="expand")
    out[["income_tax", "church_tax", "soli"]] = tax_components

    net_income = (
        out[input_col]
        - out["income_tax"].fillna(0)
        - out["church_tax"].fillna(0)
        - out["soli"].fillna(0)
    )

    out[output_col] = np.maximum(net_income, 0)
    return out



def apply_income_deduction(
    df: pd.DataFrame,
    allowance_table: pd.DataFrame,
    sociodemographics_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Applies income deductions based on allowances for the individual, their partner, and children,
    as defined in the allowance_table. Merges in partner and child info from sociodemographics_df.
    Returns a DataFrame with an 'excess_income' column and removes intermediate columns.
    """

    # Prepare allowance table
    tab = allowance_table.rename(columns={
        "Valid from": "valid_from",
        "§ 23 (1) 1": "allowance_self",
        "§ 23 (1) 2": "allowance_spouse",
        "§ 23 (1) 3": "allowance_child"
    }).copy()

    tab["valid_from"] = pd.to_datetime(tab["valid_from"])
    for col in ["allowance_self", "allowance_spouse", "allowance_child"]:
        tab[col] = pd.to_numeric(tab[col], errors="coerce")
    tab = tab.sort_values("valid_from").ffill()

    # Merge allowance table by survey year
    out = df.copy()
    out["syear_date"] = pd.to_datetime(out["syear"].astype(str) + "-01-01")
    out = pd.merge_asof(
        out.sort_values("syear_date"),
        tab[["valid_from", "allowance_self", "allowance_spouse", "allowance_child"]],
        left_on="syear_date",
        right_on="valid_from",
        direction="backward"
    )

    # Merge in has_partner and num_children from sociodemographics_df
    demo = sociodemographics_df[["pid", "syear", "has_partner", "num_children"]].copy()
    out = out.merge(demo, on=["pid", "syear"], how="left")

    # Monthly income calculation
    out["net_monthly_income"] = out["net_annual_income"] / 12

    # Fill missing values and enforce integer type
    out["has_partner"] = out["has_partner"].fillna(0).astype(int)
    out["num_children"] = out["num_children"].fillna(0).astype(int)

    # Total allowance calculation
    out["total_allowance"] = (
        out["allowance_self"]
        + out["has_partner"] * out["allowance_spouse"]
        + out["num_children"] * out["allowance_child"]
    )

    # Compute excess income after deduction
    out["excess_income"] = np.maximum(
        out["net_monthly_income"] - out["total_allowance"], 0
    )

    return out.drop(columns=[
        "valid_from", "syear_date",
        "allowance_self", "allowance_spouse", "allowance_child"
    ])


def apply_entity_allowances(
    df: pd.DataFrame,
    allowance_table: pd.DataFrame,
    *,
    entity_keys: dict[str, str],  # {'self': 'allowance_self', 'spouse': 'allowance_spouse', ...}
    entity_counts: dict[str, str],  # {'spouse': 'has_partner', 'child': 'num_children'}
    income_col: str,
    output_col: str,
    valid_from_col: str = "Valid from",
    date_col: str = "syear",
    merge_on: list[str] = ["pid", "syear"]
) -> pd.DataFrame:
    """
    Generalized allowance deduction logic.
    """
    out = df.copy()

    # Prepare allowance table
    tab = allowance_table.rename(columns={valid_from_col: "valid_from"}).copy()
    tab["valid_from"] = pd.to_datetime(tab["valid_from"])
    tab = tab.sort_values("valid_from").ffill()

    # Merge allowance table by time
    out["syear_date"] = pd.to_datetime(out[date_col].astype(str) + "-01-01")
    out = pd.merge_asof(
        out.sort_values("syear_date"),
        tab[["valid_from"] + list(entity_keys.values())],
        left_on="syear_date",
        right_on="valid_from",
        direction="backward"
    )

    # Compute total allowance
    out["total_allowance"] = out[entity_keys["self"]]
    for entity, allowance_col in entity_keys.items():
        if entity == "self":
            continue
        count_col = entity_counts[entity]
        if count_col in out:
            out[entity] = out[count_col].fillna(0).astype(int)
        else:
            out[entity] = 0
        out["total_allowance"] += out[entity] * out[allowance_col]

    out[output_col] = np.maximum(out[income_col] - out["total_allowance"], 0)

    return out.drop(columns=["valid_from", "syear_date"] + list(entity_keys.values()))
