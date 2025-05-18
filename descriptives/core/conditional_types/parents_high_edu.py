import pandas as pd 


PARENT_HIGH_EDU_LABELS = {
    0: "No parent high education",
    1: "At least one parent high education"
}

def non_take_up_by_parent_high_edu_per_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Only keep 0/1 and drop missing
    df = df[df["parent_high_edu"].isin([0, 1])]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for code, label in PARENT_HIGH_EDU_LABELS.items():
        sub = df[df["parent_high_edu"] == code]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
