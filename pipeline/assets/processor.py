import pandas as pd
import numpy as np

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle



def create_dataframe(
    df: pd.DataFrame,
    *,
    students_df: pd.DataFrame,
    policy: PolicyTableBundle,
    data: SOEPDataBundle,
) -> pd.DataFrame:
    out = df.copy()

    # bring in age (and partner/child flags if needed) from students_df
    # assume students_df has columns: pid, syear, age, has_partner (0/1), num_children
    out = out.merge(
        students_df[['pid','syear','age','has_partner','num_children']],
        on=['pid','syear'],
        how='left'
    )

    # count raw pwealth matches
    raw_matches = out.merge(
        data.pwealth[['pid', 'syear']],
        on=['pid', 'syear'],
        how='inner'
    ).shape[0]

    # interpolate & merge assets
    out = merge_assets(out, data.pwealth)

    # report gain
    interpolated_matches = out['total_assets'].notna().sum()
    print(f"Gained {interpolated_matches - raw_matches} observations through interpolation")

    # apply §29 allowance logic
    out = compute_asset_excess_allowance(
        out,
        policy.allowance_29,
        year_col="syear",
        valid_from_col="Valid From",
        age_col="age",
        under30_col="Student_U30",
        above30_col="Student_A30",
        spouse_allowance_col="Spouse_Allowance",
        child_allowance_col="Child_Allowance",
        asset_col="total_assets",
        output_col="excess_assets"
    )

    return out


def merge_assets(
    df: pd.DataFrame,
    pwealth_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    ...as before, but only produces `total_assets` (no `sum_assets`).
    """
    df = df.copy()
    base_cols = ["pid", "syear"]

    asset_groups = {
        "financial_assets": ["f0100a","f0100b","f0100c","f0100d","f0100e"],
        "real_estate":       ["e0111a","e0111b","e0111c","e0111d","e0111e"],
        "business_assets":   ["b0100a","b0100b","b0100c","b0100d","b0100e"],
        "private_insurances":["i0100a","i0100b","i0100c","i0100d","i0100e"],
        "vehicles":          ["v0100a","v0100b","v0100c","v0100d","v0100e"],
        "tangible_assets":   ["t0100a","t0100b","t0100c","t0100d","t0100e"],
        "debts":             ["w0011a","w0011b","w0011c","w0011d","w0011e"],
    }

    # ---- PREP & CLEAN ----
    all_raw = [c for cols in asset_groups.values() for c in cols]
    p = pwealth_df[base_cols + all_raw].copy()
    invalid = {-1,-3,-4,-5,-6,-7,-8}
    for cols in asset_groups.values():
        p[cols] = p[cols].where(~p[cols].isin(invalid), np.nan)
    for key, cols in asset_groups.items():
        p[key] = p[cols].mean(axis=1)
    p = p[base_cols + list(asset_groups.keys())]

    # ---- INTERPOLATE ----
    years = np.arange(df.syear.min(), df.syear.max()+1)
    idx = pd.MultiIndex.from_product([p.pid.unique(), years], names=base_cols)
    p = (
        p.set_index(base_cols)
         .reindex(idx)
         .groupby(level='pid')
         .transform(lambda x: x.interpolate('linear', limit_direction='both'))
         .fillna(0)
    )

    # ---- AGGREGATE NET ASSETS ----
    p['total_assets'] = (
        p['financial_assets']
        + p['real_estate']
        + p['business_assets']
        + p['private_insurances']
        + p['vehicles']
        + p['tangible_assets']
        - p['debts']
    )

    # ---- MERGE BACK ----
    p = p.reset_index()
    keep = base_cols + list(asset_groups.keys()) + ['total_assets']
    out = df.merge(p[keep], on=base_cols, how='left')

    # final fill
    return out.fillna(0)


def compute_asset_excess_allowance(
    df: pd.DataFrame,
    allowance_29: pd.DataFrame,
    *,
    year_col: str,
    valid_from_col: str,
    age_col: str,
    under30_col: str,
    above30_col: str,
    spouse_allowance_col: str,
    child_allowance_col: str,
    asset_col: str,
    output_col: str,
    keep_total_allowance: bool = True
) -> pd.DataFrame:
    """
    1) as-of merge only the needed cols from §29 table
    2) pick Student_U30 vs Student_A30 based on age
    3) add spouse & child allowances
    4) compute asset_excess = max(assets - allowance, 0)
    5) drop everything except the new columns you want
    """
    out = df.copy()

    # 1) Subset & rename only the cols you need for merge
    needed = [
        valid_from_col,
        under30_col,
        above30_col,
        spouse_allowance_col,
        child_allowance_col
    ]
    tab = (
        allowance_29[needed]
        .rename(columns={
            valid_from_col: 'valid_from',
            under30_col:    'allow_u30',
            above30_col:    'allow_a30',
            spouse_allowance_col: 'allow_spouse',
            child_allowance_col:  'allow_child'
        })
        .assign(valid_from=lambda d: pd.to_datetime(d['valid_from']))
        .sort_values('valid_from')
        .ffill()
    )

    # 2) as-of merge on year
    out['syear_date'] = pd.to_datetime(out[year_col].astype(str) + '-01-01')
    out = pd.merge_asof(
        out.sort_values('syear_date'),
        tab,
        left_on='syear_date',
        right_on='valid_from',
        direction='backward'
    )

    # 3) pick student allowance by age
    out['base_allowance29'] = np.where(
        out[age_col] < 30,
        out['allow_u30'],
        out['allow_a30']
    )

    # 4) total §29 allowance
    out['total_allowance29'] = (
        out['base_allowance29']
        + out['allow_spouse'] * out.get('has_partner', 0)
        + out['allow_child']  * out.get('num_children', 0)
    )

    # 5) compute asset excess
    out[output_col] = (out[asset_col] - out['total_allowance29']).clip(lower=0)

    # 6) decide which allowance columns to keep
    to_keep = [output_col]
    if keep_total_allowance:
        to_keep.append('total_allowance29')

    # 7) drop all helper and irrelevant columns
    return (
        out
        .drop(columns=[
            'syear_date', 'valid_from',
            'allow_u30', 'allow_a30',
            'allow_spouse', 'allow_child',
            'base_allowance29'
        ])
        .loc[:, list(df.columns) + to_keep]
    )
