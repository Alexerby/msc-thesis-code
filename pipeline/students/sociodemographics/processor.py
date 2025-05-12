import pandas as pd 
from .count_own_children import get_bioparen_child_counts, get_biol_child_counts


def add_sociodemographics(
    df: pd.DataFrame,
    ppath_df: pd.DataFrame,
    region_df: pd.DataFrame,
    hgen_df: pd.DataFrame,
    bioparen_df: pd.DataFrame,
    biol_df: pd.DataFrame
) -> pd.DataFrame:
    return (
        df.copy()
        .pipe(add_age, ppath_df)
        .pipe(add_bundesland, region_df)
        .pipe(add_east_background)
        .pipe(add_household_type, hgen_df)
        .pipe(add_parent_ids, bioparen_df)
        .pipe(add_lives_at_home_flag, ppath_df)
        .pipe(add_partner_flag)
        .pipe(add_child_count, bioparen_df, biol_df)
    )


def add_age(df: pd.DataFrame, ppath_df: pd.DataFrame) -> pd.DataFrame:
    p = ppath_df[["pid", "syear", "gebjahr", "gebmonat"]].copy()
    p["age"] = p["syear"] - p["gebjahr"] - (p["gebmonat"] > 6).astype(int)
    return (
        df.drop(columns=["gebjahr", "gebmonat"], errors="ignore")
          .merge(p[["pid", "syear", "age"]], on=["pid", "syear"], how="left")
    )


def add_bundesland(df: pd.DataFrame, region_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(region_df[["hid", "syear", "bula"]], on=["hid", "syear"], how="left")


def add_east_background(df: pd.DataFrame) -> pd.DataFrame:
    east_states = {11, 12, 13, 14, 15, 16}
    df["east_background"] = df["bula"].isin(east_states).astype(int)
    return df


def add_household_type(df: pd.DataFrame, hgen_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(hgen_df[["hid", "syear", "hgtyp1hh"]], on=["hid", "syear"], how="left")


def add_parent_ids(df: pd.DataFrame, bioparen_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(bioparen_df[["pid", "fnr", "mnr"]], on="pid", how="left")


def add_lives_at_home_flag(df: pd.DataFrame, ppath_df: pd.DataFrame) -> pd.DataFrame:
    parent_hids = ppath_df[["pid", "syear", "hid"]].copy()
    father_hid = parent_hids.rename(columns={"pid": "fnr", "hid": "father_hid"})
    mother_hid = parent_hids.rename(columns={"pid": "mnr", "hid": "mother_hid"})

    df = df.merge(father_hid, on=["fnr", "syear"], how="left")
    df = df.merge(mother_hid, on=["mnr", "syear"], how="left")

    parental_types = {3, 4, 5}  # Couple w/ children, single parent, multi-gen

    df["lives_at_home"] = (
        ((df["hid"] == df["father_hid"]) | (df["hid"] == df["mother_hid"])) &
        (df["hgtyp1hh"].isin(parental_types))
    ).astype(int)

    return df.drop(columns=["father_hid", "mother_hid"])


def add_partner_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'has_partner' = 1 if the person has a spouse/partner or probable one (codes 1–4), 0 otherwise.
    Always overwrites any existing 'has_partner' column with clean integer 0/1 values.
    """
    out = df.copy()
    partner_codes = {1, 3}

    # Convert partner column to numeric, coerce invalid entries to NaN
    partner_col = pd.to_numeric(out["partner"], errors="coerce")

    # Compute as 1 if in partner_codes, else 0
    out["has_partner"] = partner_col.apply(lambda x: int(x in partner_codes) if pd.notnull(x) else 0)

    return out


def add_child_count(
    df: pd.DataFrame,
    bioparen_df: pd.DataFrame,
    biol_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Combines bioparen and biol-based child information into a unified column:
    - Uses bioparen (fnr/mnr) to count number of known biological children (authoritative)
    - Falls back to forward-filled biol variable (e.g., lb0285) when bioparen info is missing
    Returns:
        df with one additional column: num_children_final
    """
    df_out = df.copy()

    # Merge bioparen-based child count
    bioparen_counts = get_bioparen_child_counts(bioparen_df)
    df_out = df_out.merge(
        bioparen_counts, how="left", left_on="pid", right_on="parent_pid"
    ).drop(columns=["parent_pid"])

    # Merge biol-based child count
    biol_ffilled = get_biol_child_counts(biol_df)
    df_out = df_out.merge(
        biol_ffilled, how="left", on=["pid", "syear"]
    )

    # Combine both sources
    df_out["num_children_final"] = (
        df_out["num_children_bioparen"]
        .combine_first(df_out["num_children_biol"])
        .fillna(0)
        .astype(int)
    )

    return df_out.drop(columns=["num_children_bioparen", "num_children_biol"])
