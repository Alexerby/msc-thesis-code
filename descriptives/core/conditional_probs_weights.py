import pandas as pd
import numpy as np
from descriptives.helpers import load_data
from tabulate import tabulate



def compute_conditional_probs_by_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df.dropna(subset=["syear"])
    if "phrf" not in df:
        raise ValueError("Weight variable 'phrf' not found in DataFrame.")

    results = []

    for syear, group in df.groupby("syear"):
        row = {"syear": syear}
        for m in [0, 1]:
            g = group[group["M"] == m]
            w = g["phrf"].fillna(0)
            for r in [0, 1]:
                mask = (g["R"] == r)
                key = f"P(R={r} | M={m})"
                prob = 100 * np.average(mask, weights=w) if w.sum() > 0 else pd.NA
                row[key] = prob
        results.append(row)

    return pd.DataFrame(results).sort_values("syear")



def compute_overall_conditional_probs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    if "phrf" not in df:
        raise ValueError("Weight variable 'phrf' not found in DataFrame.")

    results = {}
    for m in [0, 1]:
        g = df[df["M"] == m]
        w = g["phrf"].fillna(0)
        for r in [0, 1]:
            mask = (g["R"] == r)
            key = f"P(R={r} | M={m})"
            prob = 100 * np.average(mask, weights=w) if w.sum() > 0 else pd.NA
            results[key] = prob

    return pd.DataFrame([results])


def main():
    df = load_data("bafoeg_calculations", from_parquet=True)
    cond_probs_df = compute_overall_conditional_probs(df)
    cond_probs_by_year = compute_conditional_probs_by_year(df)

    print("\n📊 Conditional probabilities grouped by `syear`:\n")
    print(tabulate(cond_probs_by_year, headers="keys", tablefmt="github", floatfmt=".3f"))

    print("\n📊 Conditional probabilities average over all 'syear':\n")
    print(tabulate(cond_probs_df, headers="keys", tablefmt="github", floatfmt=".3f"))

if __name__ == "__main__":
    main()
