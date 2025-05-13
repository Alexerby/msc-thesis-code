import seaborn as sns
import os
import matplotlib.pyplot as plt
import pandas as pd

from misc.decorators import log_plot_saving


@log_plot_saving
def plot_need_components_over_time(df: pd.DataFrame, save_path: str | None = None):
    """
    Plot the evolution of BAföG need components over survey years.
    """
    components = [
        "base_need", 
        "housing_allowance",
        "insurance_supplement", 
        "total_base_need"
    ]

    # Friendly labels for legend
    component_labels = {
        "base_need": "Base Need",
        "housing_allowance": "Housing Allowance",
        "insurance_supplement": "Insurance Supplement",
        "total_base_need": "Total Base Need"
    }

    # Group and reshape data
    grouped = df.groupby("syear")[components].mean().reset_index()
    melted = pd.melt(grouped, id_vars="syear", var_name="component", value_name="value")
    melted["component"] = melted["component"].map(component_labels)

    # Plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=melted,
        x="syear",
        y="value",
        hue="component",
        style="component",
        markers=True,
        dashes=False,
    )

    plt.title("Average BAföG Need Components Over Time")
    plt.ylabel("EUR per month")
    plt.xlabel("Survey Year")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()


@log_plot_saving
def plot_reported_bafoeg_amounts_over_time(df: pd.DataFrame, save_path: str | None = None):
    """
    Plot the average reported BAföG amount (plc0168_h) over survey years.
    
    Parameters:
    - df: DataFrame containing 'syear' and 'plc0168_h'
    - save_path: Optional file path to save the plot instead of displaying it
    """
    # Filter to valid (non-missing) amounts
    valid = df[df["plc0168_h"].notna()]

    # Group by year and calculate mean
    grouped = valid.groupby("syear")["plc0168_h"].mean().reset_index()

    # Plot
    plt.figure(figsize=(10, 5))
    sns.lineplot(
        data=grouped,
        x="syear",
        y="plc0168_h",
        marker="o"
    )

    plt.title("Average Reported BAföG Amount Over Time")
    plt.ylabel("Reported Amount (EUR per month)")
    plt.xlabel("Survey Year")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()


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
