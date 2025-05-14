import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_variable(
    df, var, plot_type, groupby=None, timevar=None, hue=None, 
    save_path=None, dpi=300, drop_zeros=True, **kwargs
):
    """
    Plots a variable from a DataFrame based on the plot_type.
    
    Parameters
    ----------
    df : pd.DataFrame
        The data source.
    var : str
        Variable to plot.
    plot_type : str
        One of 'scatter', 'pdf', or 'timeline'.
    groupby : str or list, optional
        Column(s) to group by (for timeline).
    timevar : str, optional
        Time variable on x-axis.
    hue : str, optional
        Variable to color by.
    save_path : str, optional
        If given, saves the plot to this path instead of displaying it.
    dpi : int
        Resolution of the saved figure.
    drop_zeros : bool
        Whether to drop rows where var == 0.
    kwargs : dict
        Additional keyword arguments passed to seaborn.
    """
    # Drop NaNs and optionally zeros
    df = df.dropna(subset=[var])
    if drop_zeros:
        df = df[df[var] != 0]

    plt.figure()

    if plot_type == 'scatter':
        assert timevar is not None, "Need timevar for scatterplot."
        sns.scatterplot(data=df, x=timevar, y=var, hue=hue, **kwargs)
        plt.title(f'Scatterplot of {var} over {timevar}')
        
    elif plot_type == 'pdf':
        sns.histplot(df[var], kde=True, stat='density', **kwargs)
        plt.title(f'PDF of {var}')
        
    elif plot_type == 'timeline':
        assert timevar is not None, "Need timevar for timeline plot."
        if groupby:
            grouped = df.groupby([timevar, groupby])[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, hue=groupby, **kwargs)
        else:
            grouped = df.groupby(timevar)[var].mean().reset_index()
            sns.lineplot(data=grouped, x=timevar, y=var, **kwargs)
        plt.title(f'Mean {var} over time')

    else:
        raise ValueError(f"Unknown plot_type: {plot_type}")

    plt.tight_layout()

    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=dpi)
        plt.close()
    else:
        plt.show()
