
import pandas as pd

SEX_LABELS = {
    1: "Male",
    2: "Female"
}

def non_take_up_by_sex_per_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-take-up rates (P(R=0 | M=1)) by sex (1=male, 2=female) and survey year.
    Returns DataFrame: rows=years, columns=sex categories (Male, Female).
    """
    df = df.copy()
    df = df[df["sex"].isin(SEX_LABELS.keys())]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for sx, label in SEX_LABELS.items():
        sub = df[df["sex"] == sx]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})
            out[year][label] = val
    # Make sure both columns always appear
    for year in out:
        for label in SEX_LABELS.values():
            out[year].setdefault(label, pd.NA)

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
