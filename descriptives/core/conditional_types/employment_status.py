import pandas as pd 

PGEMPLST_LABELS = {
    1: "Full-Time Employment",
    2: "Regular Part-Time Employment",
    3: "Vocational Training",
    4: "Marginal/Irregular Part-Time",
    5: "Not Employed",
    6: "Sheltered Workshop",
    7: "Short-Time Work",
}


def clean_pgemplst(df):
    """Keep only valid employment status and clean."""
    df = df.copy()
    valid_codes = list(PGEMPLST_LABELS.keys())
    df = df[df["pgemplst"].isin(valid_codes)]
    # Ensure year is int and not missing or string
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)
    return df

def non_take_up_by_pgemplst(df):
    """
    Compute non-take-up rates (P(R=0 | M=1)) by employment status and year.
    Only uses valid (positive) pgemplst codes.
    Returns a DataFrame with years as rows and employment statuses as columns.
    """
    df = df.copy()
    valid_codes = [1, 2, 3, 4]  # Only categories 1–4 if you wish
    status_labels = {k: v for k, v in PGEMPLST_LABELS.items() if k in valid_codes}
    df = df[df["pgemplst"].isin(valid_codes)]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]  # Restrict to theoretically eligible
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    # Prepare output as dict-of-dicts: {year: {status_label: value, ...}, ...}
    out = {}
    for code, label in status_labels.items():
        sub = df[df["pgemplst"] == code]
        by_year = sub.groupby("syear")["R"].apply(lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA)
        for year, val in by_year.items():
            out.setdefault(year, {})[label] = val

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    return table
