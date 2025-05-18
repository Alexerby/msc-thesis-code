import pandas as pd


ANY_SIBLING_BAFOG_LABELS = {
    0: "No sibling received BAföG",
    1: "Sibling received BAföG"
}

def non_take_up_by_any_sibling_bafog_per_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Only keep 0 and 1, drop missing
    df = df[df["any_sibling_bafog"].isin([0, 1])]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for code, label in ANY_SIBLING_BAFOG_LABELS.items():
        sub = df[df["any_sibling_bafog"] == code]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
