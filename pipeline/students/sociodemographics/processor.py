from os import walk
import pandas as pd 
from .count_own_children import get_bioparen_child_counts, get_biol_child_counts

from pipeline.soep_bundle import SOEPDataBundle

from pipeline.common.processor_sociodemographics import add_age

def add_sociodemographics(df: pd.DataFrame, data: SOEPDataBundle) -> pd.DataFrame:
    return (
        df.copy()
        .pipe(add_age, data.ppath)
        .pipe(add_bundesland, data.region)
        .pipe(add_east_background)
        .pipe(add_household_type, data.hgen)

        # Family identifiers (pids)
        .pipe(add_parent_pids, data.bioparen)
        .pipe(add_current_partner_pid, data.pgen)

        .pipe(add_lives_at_home_flag, data.ppath)
        .pipe(add_partner_flag)
        .pipe(add_child_count, data.bioparen, data.biol)
        .pipe(add_siblings_pid, data.biosib)
        .pipe(add_employment_status, data.pgen)
    )



def add_employment_status(df: pd.DataFrame, pgen_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(pgen_df[["pid", "syear", "pgemplst"]], on=["pid", "syear"], how="left")


def add_siblings_pid(df: pd.DataFrame, biosib_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds sibling information to the student-level DataFrame.

    For each student (pid), extracts the list of known sibling pids from the
    `biosib` dataset and computes the number of valid known siblings.
    
    Invalid codes (e.g., -2 in SOEP) and missing values are excluded.

    Parameters
    ----------
    df : pd.DataFrame
        Student-level DataFrame with one row per pid.
    biosib_df : pd.DataFrame
        SOEP `biosib` dataset containing columns 'sibpnr1' to 'sibpnr11'.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with two new columns:
          - 'sibling_pids': list of valid sibling pids
          - 'num_known_siblings': count of valid sibling pids
    """

    # Create a list of all sibpnr columns
    sib_cols = [col for col in biosib_df.columns if col.startswith("sibpnr")]

    # Melt to long format to gather all siblings under one column
    long_df = (
        biosib_df[["pid"] + sib_cols]
        .set_index("pid")
        .stack()
        .reset_index()
        .rename(columns={"level_1": "sib_col", 0: "sibling_pid"})
    )

    # Drop NA and filter out invalid sibling pids (SOEP missing codes: e.g., -2)
    long_df = long_df.dropna(subset=["sibling_pid"])
    long_df = long_df[long_df["sibling_pid"] > 0]

    # Group by pid to collect valid sibling pids into lists
    sibling_df = (
        long_df.groupby("pid")["sibling_pid"]
        .agg(list)
        .reset_index()
        .rename(columns={"sibling_pid": "sibling_pids"})
    )

    sibling_df["num_known_siblings"] = sibling_df["sibling_pids"].apply(len)

    # Merge with original df
    return (
        df.merge(sibling_df, on="pid", how="left")
          .assign(
              sibling_pids=lambda d: d["sibling_pids"].apply(lambda x: x if isinstance(x, list) else []),
              num_known_siblings=lambda d: d["num_known_siblings"].fillna(0).astype(int)
          )
    )


def add_current_partner_pid(df: pd.DataFrame, pgen_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the current partner's PID into the dataset based on SOEP partnership indicators.

    This function merges the `pgpartnr` variable from the SOEP `pgen` dataset onto the 
    input DataFrame. `pgpartnr` indicates the PID of the respondent's current partner 
    (in the same household) for a given survey year (`syear`).

    Coding of `pgpartnr`:
    - `> 0`: PID of the partner (valid partner link)
    - `-2`: Respondent has no spouse or cohabiting partner in the household (i.e. unpartnered)
    - `NaN`: No person-level interview for the given year (respondent not in `pgen` that year)

    Parameters:
    - df (pd.DataFrame): A DataFrame containing at least 'pid' and 'syear' columns.
    - pgen_df (pd.DataFrame): The SOEP `pgen` dataset containing 'pid', 'syear', and 'pgpartnr'.

    Returns:
    - pd.DataFrame: The input DataFrame with an added 'pgpartnr' column indicating
      the current partner's PID (or appropriate code if not partnered or missing).
    """
    pgen = pgen_df[["pid", "syear", "pgpartnr"]].copy()
    return df.merge(pgen, on=["pid", "syear"], how="left")


def add_bundesland(df: pd.DataFrame, region_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(region_df[["hid", "syear", "bula"]], on=["hid", "syear"], how="left")


def add_east_background(df: pd.DataFrame) -> pd.DataFrame:
    east_states = {11, 12, 13, 14, 15, 16}
    df["east_background"] = df["bula"].isin(east_states).astype(int)
    return df


def add_household_type(df: pd.DataFrame, hgen_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(hgen_df[["hid", "syear", "hgtyp1hh"]], on=["hid", "syear"], how="left")


def add_parent_pids(df: pd.DataFrame, 
                   bioparen_df: pd.DataFrame,
                   ) -> pd.DataFrame:
    """https://companion.soep.de/Data%20Structure%20of%20SOEPcore/Data%20Identifier.html#individual-identifier-mother-father-mnr-fnr
    """
    #TODO: Some values here will be empty because the merge 
    # did not find any fnr/mnr in the right_df, but what does a value of -1 tell us?
    # Empty cell    -> No merge found in bioparen 
    # -1            -> -1 (explicitly in dataset that parent is not in SOEP dataset?)
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
    Add a binary 'has_partner' column indicating whether the person has a spouse/partner.

    This function sets 'has_partner' = 1 if the individual is coded as having a spouse/partner 
    or probable partner (based on specific partner codes, e.g., 1 or 3), and 0 otherwise.
    It always overwrites any existing 'has_partner' column with clean integer 0/1 values.

    Note:
        This column does **not** correspond to 'pgpartnr', which provides the personal ID 
        (pid) of the partner if that partner is also part of the SOEP sample. 
        'has_partner' simply reflects whether the individual is considered to have a partner 
        based on survey coding, regardless of whether the partner is in the dataset.

    Args:
        df (pd.DataFrame): Input DataFrame with a 'partner' column.

    Returns:
        pd.DataFrame: A copy of the input DataFrame with an added 'has_partner' column (0 or 1).
    """
    out = df.copy()
    partner_codes = {1, 3}

    # Convert partner column to numeric, coerce invalid entries to NaN
    partner_col = pd.to_numeric(out["partner"], errors="coerce")

    # Compute as 1 if in partner_codes, else 0
    out["has_partner"] = partner_col.apply(lambda x: int(x in partner_codes) if pd.notnull(x) else 0)

    return out


#TODO: Update the child function to use biobirth kidnr01 -- kidnr19

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
    df_out["num_children"] = (
        df_out["num_children_bioparen"]
        .combine_first(df_out["num_children_biol"])
        .fillna(0)
        .astype(int)
    )

    # TODO: Check if we can improve logic
    df_out["num_children"] = df_out["num_children"].mask(df_out["num_children"] < 0, 0)

    return df_out.drop(columns=["num_children_bioparen", "num_children_biol"])
