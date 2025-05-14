import pandas as pd
from tabulate import tabulate
from ..helpers import load_data


def compare_theoretical_and_reported(df: pd.DataFrame) -> pd.DataFrame:
    """Compare modeled vs. reported BAföG by year."""
    
    def share_nonzero(series):
        return (series > 0).mean()

    summary = df.groupby("syear").agg(
        avg_theoretical_bafög=("theoretical_bafög", "mean"),
        avg_reported_bafög=("plc0168_h", "mean"),
        share_theoretical_positive=("theoretical_bafög", share_nonzero),
        share_reported_positive=("plc0168_h", share_nonzero),
    )

    summary["take_up_rate"] = (
        summary["share_reported_positive"] / summary["share_theoretical_positive"]
    ).round(3)

    return summary.reset_index()


def reported_bafög_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize distribution of reported BAföG (plc0168_h > 0) by year."""
    df_nonzero = df[df["plc0168_h"] > 0]

    summary = df_nonzero.groupby("syear").agg(
        min_bafög=("plc0168_h", "min"),
        max_bafög=("plc0168_h", "max"),
        median_bafög=("plc0168_h", "median"),
        mean_bafög=("plc0168_h", "mean")
    )

    return summary.reset_index()


if __name__ == "__main__":
    df = load_data("bafoeg_calculations", from_parquet=True)

    print("\n### Modeled vs. Reported BAföG by Year")
    result = compare_theoretical_and_reported(df)
    print(tabulate(result, headers="keys", tablefmt="github", floatfmt=".2f"))

    print("\n### Reported BAföG Distribution (plc0168_h > 0)")
    dist = reported_bafög_distribution(df)
    print(tabulate(dist, headers="keys", tablefmt="github", floatfmt=".2f"))
