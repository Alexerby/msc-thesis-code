import pandas as pd

from .need_components import merge_need_amounts 
from .reported_amount import merge_reported_bafög

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle


def create_dataframe(
    df: pd.DataFrame, 
    policy: PolicyTableBundle,
    data: SOEPDataBundle
) -> pd.DataFrame:
    """
    Create a DataFrame containing calculated BAföG need components.

    Parameters
    ----------
    df : pd.DataFrame
        Input student-level DataFrame with at least ['pid', 'syear', 'lives_at_home'].
    policy : PolicyTableBundle
        Bundle of statutory BAföG inputs (needs, insurance, etc.).
    data : SOEPDataBundle
        Bundle containing SOEP Core input DataFrames.

    Returns
    -------
    pd.DataFrame
        DataFrame enriched with BAföG need components.
    """
    out = df.copy()

    out = merge_need_amounts(out, policy.needs, policy.insurance)
    out = merge_reported_bafög(out, data)

    return out
