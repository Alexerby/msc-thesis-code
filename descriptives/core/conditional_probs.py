import pandas as pd
from descriptives.helpers import load_data
from tabulate import tabulate


def compute_conditional_probs_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute conditional probabilities Pr(R=r | M=m) for all combinations (r,m) in {0,1}²,
    grouped by survey year, expressed as percentages (0–100).
    """
    df = df.copy()
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df.dropna(subset=["syear"])

    results = []

    for syear, group in df.groupby("syear"):
        row = {"syear": syear}
        for m in [0, 1]:
            g = group[group["M"] == m]
            total = len(g)
            for r in [0, 1]:
                key = f"P(R={r} | M={m})"
                prob = 100 * (g["R"] == r).mean() if total > 0 else pd.NA
                row[key] = prob
        results.append(row)

    return pd.DataFrame(results).sort_values("syear")


def main():
    df = load_data("bafoeg_calculations", from_parquet=True)
    cond_probs_df = compute_conditional_probs_by_year(df)

    print("\n📊 Conditional probabilities grouped by `syear`:\n")
    print(tabulate(cond_probs_df, headers="keys", tablefmt="github", floatfmt=".3f"))


if __name__ == "__main__":
    main()
