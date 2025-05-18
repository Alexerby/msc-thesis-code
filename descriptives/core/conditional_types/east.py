
import pandas as pd

EAST_LABELS = {
    0: "West Germany",
    1: "East Germany"
}

def non_take_up_by_east_per_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-take-up rates (P(R=0 | M=1)) by east/west background and year.
    Returns DataFrame: rows = years, columns = "East Germany" and "West Germany".
    """
    df = df.copy()
    # Only allow valid east_background values
    df = df[df["east_background"].isin(EAST_LABELS.keys())]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for east_code, label in EAST_LABELS.items():
        sub = df[df["east_background"] == east_code]
        by_year = sub.groupby("syear")["R"].apply(
            lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA
        )
        for year, val in by_year.items():
            out.setdefault(year, {})
            out[year][label] = val
    # Always show both columns
    for year in out:
        for label in EAST_LABELS.values():
            out[year].setdefault(label, pd.NA)

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    # Convert pd.NA to np.nan for display/LaTeX
    table = table.replace({pd.NA: float("nan")})
    return table
