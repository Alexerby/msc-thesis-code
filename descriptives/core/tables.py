import pandas as pd
from tabulate import tabulate
from ..helpers import load_data


def compare_theoretical_and_reported(df: pd.DataFrame) -> pd.DataFrame:
    """Compare modeled vs. reported BAföG by year, with over‐estimate_pct added."""

    def share_nonzero(series):
        return (series > 0).mean()

    g = df.groupby("syear")

    summary = pd.DataFrame({
        "avg_theoretical_bafög": g.apply(
            lambda x: x.loc[x["theoretical_bafög"] > 0, "theoretical_bafög"].mean()
        ),
        "avg_reported_bafög": g.apply(
            lambda x: x.loc[x["reported_bafög"] > 0, "reported_bafög"].mean()
        ),
        "theoretical_take_up_rate": g["theoretical_bafög"].apply(share_nonzero),
        "reported_take_up_rate":    g["reported_bafög"].apply(share_nonzero),
    })

    # new: absolute over‐estimate
    summary["overestimate_rate"] = (
        summary["theoretical_take_up_rate"]
        - summary["reported_take_up_rate"]
    ).round(3)

    # existing ratio for reference
    summary["take_up_ratio"] = (
        summary["reported_take_up_rate"]
        / summary["theoretical_take_up_rate"]
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


def summarize_excess(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["excess_income_stu", "excess_income_par", "excess_income_assets"]
    summary = []

    for col in cols:
        nonzero = df[col] > 0
        summary.append({
            "source": col,
            "pct_nonzero": nonzero.mean(),
            "mean_excess_all": df[col].mean(),
            "mean_excess_if_pos": df.loc[nonzero, col].mean(),
            "max_excess": df[col].max()
        })

    return pd.DataFrame(summary).set_index("source")


def summarize_excess_by_year(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["excess_income_stu", "excess_income_par", "excess_income_assets"]
    records = []

    for year, group in df.groupby("syear"):
        for col in cols:
            nonzero = group[col] > 0
            records.append({
                "syear": year,
                "source": col,
                "pct_nonzero": nonzero.mean(),
                "mean_excess_all": group[col].mean(),
                "mean_excess_if_pos": group.loc[nonzero, col].mean() if nonzero.any() else 0,
                "max_excess": group[col].max()
            })

    summary = pd.DataFrame(records)
    # optional: set a MultiIndex
    summary = summary.set_index(["syear", "source"])
    return summary


def print_excess_summary_with_separators(summary: pd.DataFrame):
    """
    Prints a multi‐year excess‐income summary (with source rows) and
    injects a blank line between each year-group for clarity.
    """
    # Flatten to rows
    flat = summary.reset_index()
    display_rows = []
    last_year = None

    for _, row in flat.iterrows():
        year = row["syear"]
        # insert a blank separator when the year changes
        if last_year is not None and year != last_year:
            display_rows.append({col: "" for col in flat.columns})
        display_rows.append(row.to_dict())
        last_year = year

    print(
        tabulate(
            display_rows,
            headers="keys",
            tablefmt="github",
            floatfmt=".3f",
            showindex=False
        )
    )



if __name__ == "__main__":
    # Load cleaned Parquet data directly (once)
    df = load_data("bafoeg_calculations", from_parquet=True)

    print("\n### Modeled vs. Reported BAföG by Year")
    result = compare_theoretical_and_reported(df)
    print(tabulate(result, headers="keys", tablefmt="github", floatfmt=".2f"))

    print("\n### Reported BAföG Distribution (reported_bafög > 0)")
    dist = reported_bafög_distribution(df)
    print(tabulate(dist, headers="keys", tablefmt="github", floatfmt=".2f"))


    print("\n### Excess‐Income Summary by Year\n")
    summary = summarize_excess_by_year(df)
    print_excess_summary_with_separators(summary)
