import pandas as pd


# Map household types
HG_LABELS = {
    1: "1-Person Household",
    2: "Couple Without Children",
    3: "Single Parent",
    4: "Couple With Children",
    5: "Couple With Children",
    6: "Couple With Children",
}

def clean_and_group_hgtyp(df):
    df = df.copy()
    # Exclude unwanted categories
    df = df[df["hgtyp1hh"].isin([1, 2, 3, 4, 5, 6])]
    # Group 4, 5, 6 as 4 ("Couple With Children")
    df["hgtyp_grouped"] = df["hgtyp1hh"].replace({5: 4, 6: 4})
    # Add label column
    df["hgtyp_label"] = df["hgtyp_grouped"].map(HG_LABELS)
    return df

def non_take_up_by_hgtyp_per_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-take-up rates (P(R=0 | M=1)) by grouped household type and survey year.
    Returns a DataFrame: rows = years, columns = household type labels.
    """
    df = df.copy()
    df = clean_and_group_hgtyp(df)
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    # Clean year
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for hg_code, label in HG_LABELS.items():
        # 4, 5, 6 are grouped as 4
        grouped_code = 4 if hg_code in [4, 5, 6] else hg_code
        sub = df[df["hgtyp_grouped"] == grouped_code]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
