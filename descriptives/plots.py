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
        "base_need", "housing_allowance",
        "kv_stat_mand", "pv_stat_mand",
        "insurance_supplement", "total_base_need"
    ]

    # Group and reshape data
    grouped = df.groupby("syear")[components].mean().reset_index()
    melted = pd.melt(grouped, id_vars="syear", var_name="component", value_name="value")

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
