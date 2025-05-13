import pandas as pd

from .sociodemographics import add_sociodemographics
from .filters import filter_age, filter_years, filter_students


def create_dataframe(
    ppath_df: pd.DataFrame,
    region_df: pd.DataFrame,
    hgen_df: pd.DataFrame,
    bioparen_df: pd.DataFrame,
    biol_df: pd.DataFrame,
    pl_df: pd.DataFrame,
    pgen_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Builds a student-level DataFrame with sociodemographic enrichment.

    Parameters
    ----------
    ppath_df : pd.DataFrame
        Person-year-level dataset (e.g. PPATH)
    region_df : pd.DataFrame
        Regional info (e.g. Bundesland)
    hgen_df : pd.DataFrame
        Household structure info (e.g. hgtyp1hh)
    bioparen_df : pd.DataFrame
        Parent linkage dataset
    biol_df : pd.DataFrame
        Biological info (e.g. lb0285: number of children)
    pl_df : pd.DataFrame
        Person-level characteristics for identifying students
    filter_fn : Optional[Callable]
        Optional filter function (e.g. for filtering to students only)

    Returns
    -------
    pd.DataFrame
        Enriched student-level dataset
    """
    df = ppath_df.copy()

    # Optional filtering
    df = filter_students(df, pl_df)

    # Add sociodemographics
    df = add_sociodemographics(
        df,
        ppath_df=ppath_df,
        region_df=region_df,
        hgen_df=hgen_df,
        bioparen_df=bioparen_df,
        biol_df=biol_df,
        pgen_df=pgen_df
    )

    df = filter_years(df)
    df = filter_age(df, "age", age_limit=35)

    return df
