
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



def apply_entity_allowances(
    df: pd.DataFrame,
    allowance_table: pd.DataFrame,
    *,
    entity_keys: dict[str, str],  # {'student': 'allowance_student', 'spouse': ..., ...}
    entity_counts: dict[str, str],  # {'spouse': 'has_partner', 'child': 'num_children'}
    income_col: str,
    output_col: str,
    base_entity: str,  # Which entity's allowance to initialize with
    valid_from_col: str = "Valid from",
    date_col: str = "syear",
    subtract_income_from_allowance: bool = False,  # <--- NEW PARAMETER
) -> pd.DataFrame:
    """
    Generalized allowance deduction logic for students, partners, or parents.
    Performs sanity checks on entity count columns.

    Parameters
    ----------
    base_entity : str
        Key in entity_keys used to initialize the allowance (e.g. "student", "parent", "partner").
    subtract_income_from_allowance : bool
        If True, computes max(allowance - income, 0). If False, computes max(income - allowance, 0).
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
    out["total_allowance"] = out[entity_keys[base_entity]]

    for entity, allowance_col in entity_keys.items():
        if entity == base_entity:
            continue
        count_col = entity_counts.get(entity)
        if count_col and count_col in out:
            out[entity] = out[count_col].fillna(0).astype(int)

            # Sanity check
            if not ((out[entity] >= 0) & (out[entity] == out[entity].astype(int))).all():
                raise ValueError(
                    f"Column '{count_col}' contains negative or non-integer values."
                )

            if set(out[entity].dropna().unique()) - {0, 1} and "partner" in entity:
                raise ValueError(
                    f"Binary column '{count_col}' (e.g., for 'has_partner') must only contain 0 or 1."
                )
        else:
            out[entity] = 0

        out["total_allowance"] += out[entity] * out[allowance_col]

    # Core subtraction logic with direction switch
    if subtract_income_from_allowance:
        out[output_col] = np.maximum(out["total_allowance"] - out[income_col], 0)
    else:
        out[output_col] = np.maximum(out[income_col] - out["total_allowance"], 0)

    return out.drop(columns=["valid_from", "syear_date"] + list(entity_keys.values()))








