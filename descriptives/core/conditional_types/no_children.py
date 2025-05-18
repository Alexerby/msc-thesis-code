import pandas as pd


NUM_CHILD_LABELS = {
    0: "No children",
    1: "One child",
    2: "Two children",
    3: "Three and more children"
}

def group_num_children(n):
    """Collapse num_children >=3 into 3 (Three and more children)."""
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

def non_take_up_by_num_children_per_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-take-up rates (P(R=0 | M=1)) by (grouped) number of children and survey year.
    Returns DataFrame: rows=years, columns=number of children categories.
    """
    df = df.copy()
    # Collapse into 0,1,2,3+
    df["num_children_grouped"] = df["num_children"].apply(group_num_children)
    df = df[df["num_children_grouped"].isin(NUM_CHILD_LABELS.keys())]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for nc, label in NUM_CHILD_LABELS.items():
        sub = df[df["num_children_grouped"] == nc]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
