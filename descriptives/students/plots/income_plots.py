from misc.decorators import log_plot_saving
import matplotlib.pyplot as plt
import seaborn as sns

import os
import pandas as pd

@log_plot_saving
def plot_gross_income_over_time(df: pd.DataFrame, save_path: str | None = None):
    """
    Plot the average gross annual income of students over survey years with 
    standard deviation bands (±1 SD) and year-over-year first differences.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing 'syear' and 'gross_annual_income'.
    save_path : str or None
        File path to save the plot, or None to display it.
    """
    # Filter valid income entries
    valid = df[df["gross_annual_income"].notna()]

    # Group by year and compute stats
    grouped = valid.groupby("syear")["gross_annual_income"].agg(["mean", "std"]).reset_index()
    grouped.rename(columns={"mean": "avg", "std": "std"}, inplace=True)
    grouped["upper"] = grouped["avg"] + grouped["std"]
    grouped["lower"] = grouped["avg"] - grouped["std"]
    grouped["diff"] = grouped["avg"].diff()

    # Ensure all columns used for plotting are numeric
    for col in ["syear", "avg", "std", "upper", "lower", "diff"]:
        grouped[col] = pd.to_numeric(grouped[col], errors="coerce")

    # Drop rows with NaNs in any plotting-relevant columns
    grouped = grouped.dropna(subset=["syear", "avg", "lower", "upper"])

    # Create plot
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Left axis: mean income with std band
    ax1.plot(grouped["syear"], grouped["avg"], label="Mean Income", marker="o")
    ax1.fill_between(grouped["syear"], grouped["lower"], grouped["upper"], alpha=0.3, label="± 1 SD")
    ax1.set_ylabel("Gross Annual Income (EUR)")
    ax1.set_xlabel("Survey Year")
    ax1.set_title("Average Gross Annual Income of Students Over Time")

    # Right axis: first difference (income change)
    ax2 = ax1.twinx()
    ax2.plot(grouped["syear"], grouped["diff"], label="Δ Income (YoY)", linestyle="--", marker="x", color="gray")
    ax2.set_ylabel("Year-over-Year Change (€)")

    # Combine legends
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()



@log_plot_saving
def plot_gross_income_pdfs_over_time(
    df: pd.DataFrame,
    save_path: str | None = None,
):
    """
    Plot PDF (histogram with KDE) of gross annual income for each survey year.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing 'syear' and 'gross_annual_income'.
    save_path : str or None
        If provided, saves each plot as PDF under this directory; otherwise, displays interactively.
    """
    # Ensure we only plot valid, positive income values
    valid = df[df["gross_annual_income"].notna() & (df["gross_annual_income"] > 0)]

    # Loop over each year
    for year, group in valid.groupby("syear"):
        plt.figure(figsize=(8, 5))
        sns.histplot(
            group["gross_annual_income"],
            bins=50,
            kde=True,
            stat="density",
            edgecolor=None,
            color="steelblue"
        )

        plt.title(f"Distribution of Gross Annual Income (EUR) – {year}")
        plt.xlabel("Gross Annual Income")
        plt.ylabel("Density")
        plt.tight_layout()

        if save_path:
            # Ensure the path is a directory; construct filename
            os.makedirs(save_path, exist_ok=True)
            plt.savefig(os.path.join(save_path, f"gross_income_pdf_{year}.pdf"))
            plt.close()
        else:
            plt.show()






@log_plot_saving
def plot_excess_income_over_time(df: pd.DataFrame, save_path: str | None = None):
    """
    Plot the average excess income of students over survey years with 
    standard deviation bands and year-over-year first differences.
    """
    valid = df[df["excess_income"].notna()]

    grouped = valid.groupby("syear")["excess_income"].agg(["mean", "std"]).reset_index()
    grouped.rename(columns={"mean": "avg", "std": "std"}, inplace=True)
    grouped["upper"] = grouped["avg"] + grouped["std"]
    grouped["lower"] = grouped["avg"] - grouped["std"]
    grouped["diff"] = grouped["avg"].diff()

    for col in ["syear", "avg", "std", "upper", "lower", "diff"]:
        grouped[col] = pd.to_numeric(grouped[col], errors="coerce")

    grouped = grouped.dropna(subset=["syear", "avg", "lower", "upper"])

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(grouped["syear"], grouped["avg"], label="Mean Excess Income", marker="o")
    ax1.fill_between(grouped["syear"], grouped["lower"], grouped["upper"], alpha=0.3, label="± 1 SD")
    ax1.set_ylabel("Excess Monthly Income (EUR)")
    ax1.set_xlabel("Survey Year")
    ax1.set_title("Average Excess Monthly Income Over Time")

    ax2 = ax1.twinx()
    ax2.plot(grouped["syear"], grouped["diff"], label="Δ Excess Income (YoY)", linestyle="--", marker="x", color="gray")
    ax2.set_ylabel("Year-over-Year Change (€)")

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()


@log_plot_saving
def plot_excess_income_pdfs_over_time(df: pd.DataFrame, save_path: str | None = None):
    """
    Plot histogram + KDE of excess income by year.
    """
    valid = df[df["excess_income"].notna() & (df["excess_income"] > 0)]

    for year, group in valid.groupby("syear"):
        plt.figure(figsize=(8, 5))
        sns.histplot(
            group["excess_income"],
            bins=50,
            kde=True,
            stat="density",
            edgecolor=None,
            color="darkgreen"
        )

        plt.title(f"Distribution of Excess Monthly Income – {year}")
        plt.xlabel("Excess Income (EUR)")
        plt.ylabel("Density")
        plt.tight_layout()

        if save_path:
            os.makedirs(save_path, exist_ok=True)
            plt.savefig(os.path.join(save_path, f"excess_income_pdf_{year}.pdf"))
            plt.close()
        else:
            plt.show()


def summarize_excess_income(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary table of mean, std, median excess income by year.
    """
    valid = df[df["excess_income"].notna()]
    summary = valid.groupby("syear")["excess_income"].agg(
        mean="mean",
        std="std",
        median="median",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        count="count"
    ).reset_index()
    return summary
