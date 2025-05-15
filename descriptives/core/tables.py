import pandas as pd
from tabulate import tabulate
from ..helpers import load_data


def compare_theoretical_and_reported(df: pd.DataFrame) -> pd.DataFrame:
    """Compare modeled vs. reported BAföG by year with conditional averages and clear labels."""

    def share_nonzero(series):
        return (series > 0).mean()

    grouped = df.groupby("syear")

    summary = pd.DataFrame({
        "avg_theoretical_bafög": grouped.apply(
            lambda g: g.loc[g["theoretical_bafög"] > 0, "theoretical_bafög"].mean()
        ),
        "avg_reported_bafög": grouped.apply(
            lambda g: g.loc[g["reported_bafög"] > 0, "reported_bafög"].mean()
        ),
        "theoretical_take_up_rate": grouped["theoretical_bafög"].apply(share_nonzero),
        "reported_take_up_rate": grouped["reported_bafög"].apply(share_nonzero),
    })

    summary["take_up_ratio"] = (
        summary["reported_take_up_rate"] / summary["theoretical_take_up_rate"]
    ).round(3)

    return summary.reset_index()


def reported_bafög_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize distribution of reported BAföG (reported_bafög > 0) by year."""
    df_nonzero = df[df["reported_bafög"] > 0]

    summary = df_nonzero.groupby("syear").agg(
        min_bafög=("reported_bafög", "min"),
        max_bafög=("reported_bafög", "max"),
        median_bafög=("reported_bafög", "median"),
        mean_bafög=("reported_bafög", "mean")
    )

    return summary.reset_index()


if __name__ == "__main__":
    # Load cleaned Parquet data directly
    df = load_data("bafoeg_calculations", from_parquet=True)

    print("\n### Modeled vs. Reported BAföG by Year")
    result = compare_theoretical_and_reported(df)
    print(tabulate(result, headers="keys", tablefmt="github", floatfmt=".2f"))

    print("\n### Reported BAföG Distribution (reported_bafög > 0)")
    dist = reported_bafög_distribution(df)
    print(tabulate(dist, headers="keys", tablefmt="github", floatfmt=".2f"))
