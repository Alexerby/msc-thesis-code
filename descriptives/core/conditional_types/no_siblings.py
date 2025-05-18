import pandas as pd

NUM_SIBLINGS_LABELS = {
    0: "No siblings",
    1: "One sibling",
    2: "Two siblings",
    3: "Three and more siblings"
}

def group_num_siblings(n):
    """Collapse num_known_siblings >=3 into 3 (Three and more siblings)."""
    if pd.isna(n):
        return pd.NA
    try:
        n = int(n)
    except:
        return pd.NA
    if n >= 3:
        return 3
    elif n >= 0:
        return n
    else:
        return pd.NA

def non_take_up_by_num_siblings_per_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-take-up rates (P(R=0 | M=1)) by (grouped) number of known siblings and survey year.
    Returns DataFrame: rows=years, columns=number of siblings categories.
    """
    df = df.copy()
    # Collapse into 0,1,2,3+
    df["num_siblings_grouped"] = df["num_known_siblings"].apply(group_num_siblings)
    df = df[df["num_siblings_grouped"].isin(NUM_SIBLINGS_LABELS.keys())]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for ns, label in NUM_SIBLINGS_LABELS.items():
        sub = df[df["num_siblings_grouped"] == ns]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
