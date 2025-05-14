import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_variable(
    df,
    var,
    plot_type,
    groupby=None,
    timevar=None,
    hue=None,
    save_path=None,
    dpi=300,
    drop_zeros=True,
    title=None,
    **kwargs
):
    """
    Plots a variable from a DataFrame based on the plot_type.
    """
    df = df.dropna(subset=[var])
    if drop_zeros:
        df = df[df[var] != 0]

    if df.empty:
        print(f"[WARN] Skipping empty plot for {var}")
        return

    plt.figure()

    if plot_type == 'scatter':
        assert timevar is not None, "Need timevar for scatterplot."
        sns.scatterplot(data=df, x=timevar, y=var, hue=hue, **kwargs)
        plt.title(title or f'Scatterplot of {var} over {timevar}')

    elif plot_type == 'pdf':
        sns.histplot(df[var], kde=True, stat='density', **kwargs)
        plt.title(title or f'Distribution of {var}')

    elif plot_type == 'timeline':
        assert timevar is not None, "Need timevar for timeline plot."
        if groupby:
            grouped = df.groupby([timevar, groupby])[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, hue=groupby, **kwargs)
        else:
            grouped = df.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, **kwargs)
        plt.title(title or f'Mean {var} over time')

    else:
        raise ValueError(f"Unknown plot_type: {plot_type}")

    plt.tight_layout()

    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=dpi)
        plt.close()
    else:
        plt.show()


def plot_comparison(
    df,
    var1: str,
    var2: str,
    plot_type: str,
    timevar: str = None,
    hue: str = None,
    drop_zeros: bool = True,
    title1: str = None,
    title2: str = None,
    save_path: str = None,
    dpi: int = 300,
    **kwargs
):
    """
    Plot two variables side by side using the same plot_type ('pdf', 'scatter', or 'timeline').

    Parameters
    ----------
    df : pd.DataFrame
        The data to plot.
    var1, var2 : str
        The two variables to compare.
    plot_type : str
        Type of plot: 'pdf', 'scatter', or 'timeline'.
    timevar : str, optional
        Required for 'scatter' or 'timeline' plots.
    hue : str, optional
        Hue grouping (passed to seaborn).
    drop_zeros : bool
        Whether to exclude rows where variable is zero.
    title1, title2 : str
        Custom plot titles.
    save_path : str, optional
        If given, saves to disk. Otherwise shows the plot.
    dpi : int
        Resolution if saving.
    kwargs : dict
        Extra options for seaborn.
    """
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    for ax, var, title in zip(
        axes, [var1, var2], [title1 or var1, title2 or var2]
    ):
        sub = df.dropna(subset=[var])
        if drop_zeros:
            sub = sub[sub[var] != 0]

        if sub.empty:
            ax.set_title(f"No data for {var}")
            continue

        if plot_type == 'pdf':
            sns.histplot(sub[var], kde=True, stat='density', ax=ax, **kwargs)
            ax.set_title(f"PDF of {title}")
        elif plot_type == 'scatter':
            assert timevar is not None, "timevar required for scatter"
            sns.scatterplot(data=sub, x=timevar, y=var, hue=hue, ax=ax, **kwargs)
            ax.set_title(f"Scatter: {title} vs. {timevar}")
        elif plot_type == 'timeline':
            assert timevar is not None, "timevar required for timeline"
            grouped = sub.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, ax=ax, **kwargs)
            ax.set_title(f"Mean {title} over time")
        else:
            raise ValueError(f"Unknown plot_type: {plot_type}")

        ax.set_xlabel(timevar if timevar else var)
        ax.set_ylabel(var)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=dpi)
        plt.close()
    else:
        plt.show()


def plot_pdf_per_year(df, var, output_dir):
    for year in sorted(df["syear"].dropna().unique()):
        subset = df[df["syear"] == year]
        if subset[var].dropna().eq(0).all():
            continue  # skip if all zero or missing

        save_path = output_dir / f"{var}_pdf_{year}.png"

        plot_variable(
            subset,
            var=var,
            plot_type="pdf",
            title=f"{var} Distribution ({year})",
            drop_zeros=True,
            save_path=save_path
        )
