import seaborn as sns
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
