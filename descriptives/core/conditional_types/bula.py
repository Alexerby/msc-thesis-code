import pandas as pd 
import numpy as np

BULA_LABELS = {
    1: "Baden-Württemberg",
    2: "Bayern",
    3: "Berlin",
    4: "Brandenburg",
    5: "Bremen",
    6: "Hamburg",
    7: "Hessen",
    8: "Mecklenburg-Vorpommern",
    9: "Niedersachsen",
    10: "Nordrhein-Westfalen",
    11: "Rheinland-Pfalz",
    12: "Saarland",
    13: "Sachsen",
    14: "Sachsen-Anhalt",
    15: "Schleswig-Holstein",
    16: "Thüringen"
}

def non_take_up_by_bula_per_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-take-up rates (P(R=0 | M=1)) by Bundesland and survey year.
    Returns DataFrame: rows=years, columns=Bundesland names.
    """
    df = df.copy()
    # Only keep valid codes (1-16)
    df = df[df["bula"].isin(BULA_LABELS.keys())]
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df[df["M"] == 1]
    df = df[df["syear"].notna()]
    df = df[pd.to_numeric(df["syear"], errors="coerce").notna()]
    df["syear"] = df["syear"].astype(int)

    out = {}
    for bula_code, label in BULA_LABELS.items():
        sub = df[df["bula"] == bula_code]
        by_year = sub.groupby("syear")["R"].apply(
            lambda s: 100 * (s == 0).mean() if len(s) > 0 else pd.NA
        )
        for year, val in by_year.items():
            out.setdefault(year, {})
            out[year][label] = val
    # Make sure all Bundesländer always appear as columns
    for year in out:
        for label in BULA_LABELS.values():
            out[year].setdefault(label, pd.NA)

    table = pd.DataFrame.from_dict(out, orient="index")
    table.index.name = "Year"
    table = table.sort_index()
    # Convert all pd.NA to np.nan for tabulate compatibility
    table = table.replace({pd.NA: np.nan})
    return table
