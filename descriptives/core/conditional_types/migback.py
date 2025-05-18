import pandas as pd 


MIGBACK_LABELS = {
    1: "No migration background",
    2: "With migration background"  # Will group both 2 and 3 under this
}

def group_migback(val):
    if pd.isna(val):
        return pd.NA
    if val == 1:
        return 1  # No migration background
    elif val in [2, 3]:
        return 2  # With migration background
    else:
        return pd.NA

def non_take_up_by_migback_per_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["migback_grouped"] = df["migback"].apply(group_migback)
    df = df[df["migback_grouped"].isin(MIGBACK_LABELS.keys())]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for mb, label in MIGBACK_LABELS.items():
        sub = df[df["migback_grouped"] == mb]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
