import os
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
import numpy as np

def prettify(varname):
    """Replace underscores with spaces and capitalize words; map 'syear' to 'Year'."""
    if not varname:
        return ''
    if varname.lower() == "syear":
        return "Year"
    return varname.replace('_', ' ').title()


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
        plt.title(title or f'Scatterplot of {prettify(var)} Over {prettify(timevar)}')
        plt.xlabel(prettify(timevar))
        plt.ylabel(prettify(var))

    elif plot_type == 'pdf':
        # Use the 1st and 99th percentiles for limits
        data_min, data_max = np.nanpercentile(df[var], [1, 99])
        sns.histplot(
            df[var],
            kde=True,
            stat='density',
            color="black",
            fill=False,
            binrange=(data_min, data_max),
            **kwargs
        )
        plt.title(title or f'Distribution of {prettify(var)}')
        plt.xlabel(prettify(var))
        plt.ylabel("Density")
        plt.xlim(data_min, data_max)

    elif plot_type == 'timeline':
        assert timevar is not None, "Need timevar for timeline plot."
        if groupby:
            grouped = df.groupby([timevar, groupby])[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, hue=groupby, **kwargs)
            plt.xlabel(prettify(timevar))
            plt.ylabel(f"Mean {prettify(var)}")
        else:
            grouped = df.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, color="black", **kwargs)
            plt.xlabel(prettify(timevar))
            plt.ylabel(f"Mean {prettify(var)}")
        plt.title(title or f"Mean {prettify(var)} Over Time")

    else:
        raise ValueError(f"Unknown plot_type: {plot_type}")

    # === Cutoff x-axis to 1st-99th percentile for scatter and timeline ===
    if plot_type in ['timeline', 'scatter']:
        x_data = df[timevar] if timevar is not None else None
        # Numeric or datetime
        if x_data is not None and (
            np.issubdtype(x_data.dtype, np.number) or np.issubdtype(x_data.dtype, np.datetime64)
        ):
            lower, upper = np.nanpercentile(x_data, [1, 99])
            plt.xlim(lower, upper)

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
    Plot two variables side by side using APA-style aesthetics, with synchronized axes.
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

    # --- Collect axis limits across both variables ---
    x_mins, x_maxs, y_mins, y_maxs = [], [], [], []

    for var in [var1, var2]:
        sub = df.dropna(subset=[var])
        if drop_zeros:
            sub = sub[sub[var] != 0]
        if sub.empty:
            continue

        if plot_type == 'pdf':
            x_mins.append(sub[var].min())
            x_maxs.append(sub[var].max())
            # Temporarily plot to get y-limit
            tmp_ax = sns.histplot(sub[var], kde=True, stat="density", element="step", fill=False)
            y_mins.append(0)
            y_maxs.append(tmp_ax.get_ylim()[1])
            plt.cla()
        elif plot_type == 'timeline':
            assert timevar is not None, "timevar required for timeline"
            grouped = sub.groupby(timevar)[var].mean().reset_index()
            x_mins.append(grouped[timevar].min())
            x_maxs.append(grouped[timevar].max())
            y_mins.append(grouped[var].min())
            y_maxs.append(grouped[var].max())
        elif plot_type == 'scatter':
            assert timevar is not None, "timevar required for scatter"
            x_mins.append(sub[timevar].min())
            x_maxs.append(sub[timevar].max())
            y_mins.append(sub[var].min())
            y_maxs.append(sub[var].max())

    # Only set xlim/ylim if we have data
    xlim = (min(x_mins), max(x_maxs)) if x_mins and x_maxs else None
    ylim = (min(y_mins), max(y_maxs)) if y_mins and y_maxs else None

    # --- Actual plotting
    for ax, var, title in zip(
        axes, [var1, var2], [title1 or var1, title2 or var2]
    ):
        sub = df.dropna(subset=[var])
        if drop_zeros:
            sub = sub[sub[var] != 0]

        if sub.empty:
            ax.set_title(f"No data for {prettify(var)}", fontstyle="italic")
            ax.set_xlabel("")
            ax.set_ylabel("")
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
            ax.set_title(f"Distribution of {prettify(title)}")
            ax.set_xlabel(prettify(var))
            ax.set_ylabel("Density")

        elif plot_type == 'scatter':
            sns.scatterplot(
                data=sub,
                x=timevar,
                y=var,
                hue=hue,
                palette="gray" if hue else None,
                ax=ax,
                **kwargs
            )
            if xlim: ax.set_xlim(xlim)
            if ylim: ax.set_ylim(ylim)
            ax.set_title(f"{prettify(title)} vs. {prettify(timevar)}")
            ax.set_xlabel(prettify(timevar))
            ax.set_ylabel(prettify(var))

        elif plot_type == 'timeline':
            grouped = sub.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(
                data=grouped,
                x=timevar,
                y=var,
                color="black",
                ax=ax,
                **kwargs
            )
            if xlim: ax.set_xlim(xlim)
            if ylim: ax.set_ylim(ylim)
            ax.set_title(f"Mean {prettify(title)} Over Time")
            ax.set_xlabel(prettify(timevar))
            ax.set_ylabel(f"Mean {prettify(var)}")

        else:
            raise ValueError(f"Unknown plot_type: {plot_type}")

        ax.grid(True, which="major", linestyle="--", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=dpi)
        plt.close()
    else:
        plt.show()
