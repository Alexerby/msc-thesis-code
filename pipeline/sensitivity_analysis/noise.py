import numpy as np
import pandas as pd

def add_income_noise(
    df: pd.DataFrame,
    income_col: str,
    noise_std: float = 0.1,
    seed: int | None = None,
) -> pd.DataFrame:
    """
    Add additive normal noise to the log of an income column (only for positive values),
    then exponentiate to return to the original scale.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the income column.
    income_col : str
        Name of the income column to add noise to.
    noise_std : float, optional
        Standard deviation of the noise in log space (default 0.1 ~10% noise).
    seed : int or None, optional
        Random seed for reproducibility.

    Returns
    -------
    pd.DataFrame
        DataFrame with the noisy income column overwritten (positive values only).
    """
    if seed is not None:
        np.random.seed(seed)

    # Ensure the column is float for log calculation
    df[income_col] = pd.to_numeric(df[income_col], errors='coerce')

    # Mask for strictly positive values
    positive_mask = df[income_col] > 0

    # Log-transform, add noise, exponentiate
    log_income = np.log(df.loc[positive_mask, income_col].astype(float))
    noise = np.random.normal(loc=0, scale=noise_std, size=log_income.shape[0])
    df.loc[positive_mask, income_col] = np.exp(log_income + noise)

    return df
