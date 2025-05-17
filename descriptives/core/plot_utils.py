import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
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
        sns.histplot(df[var], kde=True, stat='density', color="black", fill=False, **kwargs)
        plt.title(title or f'Distribution of {var}')

    elif plot_type == 'timeline':
        assert timevar is not None, "Need timevar for timeline plot."
        if groupby:
            grouped = df.groupby([timevar, groupby])[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, hue=groupby, **kwargs)
        else:
            grouped = df.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, color="black", **kwargs)
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
    Plot two variables side by side using APA-style aesthetics.

    Parameters
    ----------
    df : pd.DataFrame
    var1, var2 : str
        The two variables to compare.
    plot_type : str
        Type of plot: 'pdf', 'scatter', or 'timeline'.
    timevar : str, optional
    hue : str, optional
    drop_zeros : bool
    title1, title2 : str
    save_path : str, optional
    dpi : int
    kwargs : dict
    """
    # --- APA-style tweaks ---
    sns.set_theme(style="whitegrid")
    mpl.rcParams.update({
        "font.size": 11,
        "font.family": "sans-serif",
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "axes.edgecolor": "black",
        "axes.linewidth": 0.8,
        "xtick.color": "black",
        "ytick.color": "black",
        "xtick.direction": "out",
        "ytick.direction": "out",
        "grid.color": "gray",
        "grid.linestyle": "--",
        "grid.linewidth": 0.5,
        "legend.frameon": False,
        "figure.dpi": dpi,
    })

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)

    # --- Determine shared axis limits if plot_type is 'pdf'
    x_mins, x_maxs, y_maxs = [], [], []

    if plot_type == 'pdf':
        for var in [var1, var2]:
            sub = df.dropna(subset=[var])
            if drop_zeros:
                sub = sub[sub[var] != 0]
            if sub.empty:
                continue
            x_mins.append(sub[var].min())
            x_maxs.append(sub[var].max())
            # Temporarily plot to get y-limit
            tmp_ax = sns.histplot(sub[var], kde=True, stat="density", element="step", fill=False)
            y_maxs.append(tmp_ax.get_ylim()[1])
            plt.cla()  # Clear temporary plot

        xlim = (min(x_mins), max(x_maxs)) if x_mins and x_maxs else None
        ylim = (0, max(y_maxs)) if y_maxs else None
    else:
        xlim, ylim = None, None

    # --- Actual plotting
    for ax, var, title in zip(
        axes, [var1, var2], [title1 or var1, title2 or var2]
    ):
        sub = df.dropna(subset=[var])
        if drop_zeros:
            sub = sub[sub[var] != 0]

        if sub.empty:
            ax.set_title(f"No data for {var}", fontstyle="italic")
            continue

        if plot_type == 'pdf':
            sns.histplot(
                sub[var],
                kde=True,
                stat='density',
                color="black",
                fill=False,
                ax=ax,
                **kwargs
            )
            if xlim: ax.set_xlim(xlim)
            if ylim: ax.set_ylim(ylim)
            ax.set_title(f"Distribution of {title}")

        elif plot_type == 'scatter':
            assert timevar is not None, "timevar required for scatter"
            sns.scatterplot(
                data=sub,
                x=timevar,
                y=var,
                hue=hue,
                palette="gray" if hue else None,
                ax=ax,
                **kwargs
            )
            ax.set_title(f"{title} vs. {timevar}")

        elif plot_type == 'timeline':
            assert timevar is not None, "timevar required for timeline"
            grouped = sub.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(
                data=grouped,
                x=timevar,
                y=var,
                color="black",
                ax=ax,
                **kwargs
            )
            ax.set_title(f"Mean {title} over time")

        else:
            raise ValueError(f"Unknown plot_type: {plot_type}")

        ax.set_xlabel(timevar if timevar else var)
        ax.set_ylabel(var)
        ax.grid(True, which="major", linestyle="--", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
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
