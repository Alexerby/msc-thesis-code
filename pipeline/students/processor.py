import pandas as pd

from .sociodemographics import add_sociodemographics
from .income import add_income

from .filters import filter_years
from pipeline.common.filters import filter_age, filter_students


from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle

def create_dataframe(data: SOEPDataBundle, policy: PolicyTableBundle) -> pd.DataFrame:
    """
    Builds a student-level DataFrame with sociodemographic enrichment.

    Parameters
    ----------
    data : SOEPDataBundle
        Bundle of relevant SOEP Core DataFrames.

    Returns
    -------
    pd.DataFrame
        Enriched student-level dataset
    """
    df = data.ppath.copy()

    # Optional filtering
    df = filter_students(df, data.pl)
    df = filter_years(df)

    # Add sociodemographics
    df = add_sociodemographics(df, data)
    df = filter_age(df, "age", age_limit=35)

    df = add_income(df, data, policy)


    return pd.DataFrame(df)
