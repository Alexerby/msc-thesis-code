import numpy as np
import pandas as pd

def add_income_noise(
    df: pd.DataFrame,
    income_col: str,
    noise_std: float = 0.1,
    seed: int | None = None,
) -> pd.DataFrame:
    """
    Add multiplicative lognormal noise to an income column directly,
    overwriting the original values.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the income column.
    income_col : str
        Name of the income column to add noise to.
    noise_std : float, optional
        Standard deviation of the lognormal noise (default 0.1 ~10% noise).
    seed : int or None, optional
        Random seed for reproducibility.

    Returns
    -------
    pd.DataFrame
        DataFrame with the noisy income column overwritten.
    """
    if seed is not None:
        np.random.seed(seed)

    noise_factors = np.random.lognormal(mean=0, sigma=noise_std, size=len(df))
    df[income_col] = df[income_col] * noise_factors

    return df
