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
    drop_small=None,
    title=None,
    trim_percentile=None,
    **kwargs
):
    """
    Plots a variable from a DataFrame based on the plot_type.

    trim_percentile: float or None
        If set, trims the given percentile from each tail (e.g. 2.5 means
        using 2.5th and 97.5th percentile) when setting x/y limits or histogram bins.
    """
    trim_percentile = kwargs.pop('trim_percentile', None) 
    df = df.dropna(subset=[var])

    if drop_zeros:
        df = df[df[var] != 0]
    if drop_small is not None:
        df = df[df[var] > drop_small]

    if df.empty:
        print(f"[WARN] Skipping empty plot for {var}")
        return

    plt.figure()

    def set_title(t):
        plt.title(t, fontsize=13, fontweight='bold', family='sans-serif')

    if plot_type == 'scatter':
        assert timevar is not None, "Need timevar for scatterplot."
        sns.scatterplot(data=df, x=timevar, y=var, hue=hue, **kwargs)
        set_title(title or f'Scatterplot of {prettify(var)} Over {prettify(timevar)}')
        plt.xlabel(prettify(timevar))
        plt.ylabel(prettify(var))

    elif plot_type == 'pdf':
        if trim_percentile is not None:
            low_p = trim_percentile
            high_p = 100 - trim_percentile
            data_min, data_max = np.nanpercentile(df[var], [low_p, high_p])
        else:
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
        set_title(title or f'Distribution of {prettify(var)}')
        plt.xlabel(prettify(var))
        plt.ylabel("Density")
        plt.xlim(data_min, data_max)

    elif plot_type == 'timeline':
        assert timevar is not None, "Need timevar for timeline plot."
        if groupby:
            grouped = df.groupby([timevar, groupby])[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, hue=groupby, marker='o', **kwargs)
            plt.xlabel(prettify(timevar))
            plt.ylabel(f"Mean {prettify(var)}")
        else:
            grouped = df.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, color="black", marker='o', **kwargs)
            plt.xlabel(prettify(timevar))
            plt.ylabel(f"Mean {prettify(var)}")

        # Add APA style gridlines:
        plt.grid(True, which='major', linestyle='--', linewidth=0.5, color='gray', alpha=0.6)
        plt.grid(False, which='minor')  # Disable minor gridlines if any

        set_title(title or f"Mean {prettify(var)} Over Time")

        # Use grouped data to set y-limits, respecting trim_percentile
        if trim_percentile is not None:
            low_p = trim_percentile
            high_p = 100 - trim_percentile
            y_lower, y_upper = np.nanpercentile(grouped[var], [low_p, high_p])
        else:
            y_lower, y_upper = np.nanpercentile(grouped[var], [1, 99])

        # Add padding (7%) on y-axis limits
        y_range = y_upper - y_lower
        padding = y_range * 0.2
        plt.ylim(y_lower - padding, y_upper + padding)

    # === Cutoff x-axis to trimmed percentile for scatter and timeline ===
    if plot_type in ['timeline', 'scatter']:
        x_data = df[timevar] if timevar is not None else None
        if x_data is not None and (
            np.issubdtype(x_data.dtype, np.number) or np.issubdtype(x_data.dtype, np.datetime64)
        ):
            if trim_percentile is not None:
                low_p = trim_percentile
                high_p = 100 - trim_percentile
                lower, upper = np.nanpercentile(x_data, [low_p, high_p])
            else:
                lower, upper = np.nanpercentile(x_data, [1, 99])
            plt.xlim(lower, upper)

        # For scatter plots, y-limits come from raw data (same as before)
        if plot_type == 'scatter':
            y_data = df[var]
            if y_data is not None and np.issubdtype(y_data.dtype, np.number):
                if trim_percentile is not None:
                    low_p = trim_percentile
                    high_p = 100 - trim_percentile
                    y_lower, y_upper = np.nanpercentile(y_data, [low_p, high_p])
                else:
                    y_lower, y_upper = np.nanpercentile(y_data, [1, 99])
                plt.ylim(y_lower, y_upper)

    plt.tight_layout()

    if save_path:
        expanded_path = os.path.expanduser(save_path)
        os.makedirs(os.path.dirname(expanded_path), exist_ok=True)
        plt.savefig(expanded_path, dpi=dpi)
        print(f"Plot saved to: {expanded_path}")
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

    # Helper function for APA style title on axes
    def set_ax_title(ax, t):
        ax.set_title(t, fontsize=13, fontweight='bold', family='sans-serif')

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
            set_ax_title(ax, f"Distribution of {prettify(title)}")
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
            set_ax_title(ax, f"{prettify(title)} vs. {prettify(timevar)}")
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
            set_ax_title(ax, f"Mean {prettify(title)} Over Time")
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


def plot_multiple_timelines(
    df,
    vars: list[str],
    timevar: str,
    drop_zeros: bool = True,
    title: str = None,
    save_path: str = None,
    dpi: int = 300,
    linewidth: float = 1.8,
    markers: list = None,
    linestyles: list = None,
    **kwargs
):
    """
    Plot multiple timeline variables on the same plot with APA-style black & white aesthetics:
    black lines with different line styles and markers to distinguish series.

    Parameters:
        df: pd.DataFrame
        vars: list of columns to plot
        timevar: x-axis variable (time)
        drop_zeros: if True, drop rows where var == 0
        title: plot title
        save_path: path to save plot (optional)
        dpi: resolution for saved figure
        linewidth: line width for each series
        markers: list of marker styles (e.g. ['o', 's', '^']), None for no markers
        linestyles: list of line styles (e.g. ['-', '--', ':', '-.'])
        kwargs: additional args passed to sns.lineplot
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    import os

    sns.set_theme(style="white")

    plt.figure(figsize=(9, 6))

    # Default line styles and markers if none provided
    if linestyles is None:
        linestyles = ['-', '--', ':', '-.']
    if markers is None:
        markers = [None] * len(vars)  # No markers by default

    for i, var in enumerate(vars):
        sub = df.dropna(subset=[var, timevar])
        if drop_zeros:
            sub = sub[sub[var] != 0]

        if sub.empty:
            print(f"[WARN] No data to plot for {var}. Skipping.")
            continue

        grouped = sub.groupby(timevar)[var].mean().reset_index()

        style = linestyles[i % len(linestyles)]
        marker = markers[i] if i < len(markers) else None

        plt.plot(
            grouped[timevar],
            grouped[var],
            label=prettify(var),
            color='black',
            linestyle=style,
            linewidth=linewidth,
            marker=marker,
            **kwargs
        )

    plt.xlabel(prettify(timevar), fontsize=11, family='sans-serif')
    plt.ylabel("Mean Value", fontsize=11, family='sans-serif')
    plt.title(title or f"Timeline of {', '.join(vars)} over {prettify(timevar)}",
              fontsize=13, fontweight='bold', family='sans-serif')

    plt.legend(frameon=False, fontsize=10)

    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(0.8)
    ax.spines['bottom'].set_linewidth(0.8)

    ax.yaxis.grid(True, linestyle='--', linewidth=0.5, color='gray', alpha=0.3)
    ax.xaxis.grid(False)

    plt.tight_layout()

    if save_path:
        expanded_path = os.path.expanduser(save_path)
        os.makedirs(os.path.dirname(expanded_path), exist_ok=True)
        plt.savefig(expanded_path, dpi=dpi)
        print(f"Plot saved to: {expanded_path}")
        plt.close()
    else:
        plt.show()
