import pandas as pd
import numpy as np
from descriptives.helpers import load_data


def compute_conditional_probs_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute conditional probabilities Pr(R=r | M=m) for all combinations (r,m) in {0,1}²,
    grouped by survey year.

    Assumes:
    - 'theoretical_bafög' (float): model eligibility (M = 1 if > 0)
    - 'receives_bafög' (0/1 or pd.NA): reported receipt (R)
    - 'syear' is present
    """
    df = df.copy()
    df["M"] = (df["theoretical_bafög"] > 0).astype(int)
    df["R"] =  df["plc0167_h"]
    df = df.dropna(subset=["R", "syear"])

    results = []

    for syear, group in df.groupby("syear"):
        row = {"syear": syear}
        for m in [0, 1]:
            g = group[group["M"] == m]
            total = len(g)
            for r in [0, 1]:
                key = f"P(R={r} | M={m})"
                prob = (g["R"] == r).mean() if total > 0 else np.nan
                row[key] = prob
        results.append(row)

    return pd.DataFrame(results).sort_values("syear")


def main():
    df = load_data("bafoeg_calculations", source="parquet")
    cond_probs_df = compute_conditional_probs_by_year(df)

    print("\n📊 Conditional probabilities grouped by `syear`:\n")
    print(cond_probs_df.to_string(index=False, float_format="%.3f"))

    # Optionally export to CSV
    # cond_probs_df.to_csv("outputs/conditional_probs_by_year.csv", index=False)


if __name__ == "__main__":
    main()
