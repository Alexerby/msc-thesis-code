import pandas as pd 


def get_bioparen_child_counts(bioparen_df: pd.DataFrame) -> pd.DataFrame:
    """
    Count the number of known children per parent using the `bioparen` dataset.

    This function identifies all parent-child relationships based on `fnr` (father pid) 
    and `mnr` (mother pid), and returns a DataFrame with the number of times each 
    parent appears as a biological parent.

    Parameters:
    - bioparen_df (pd.DataFrame): A DataFrame containing at least the columns `pid`, `fnr`, and `mnr`.

    Returns:
    - pd.DataFrame: A DataFrame with one row per parent, containing:
        - 'parent_pid': The pid of the identified parent (from fnr or mnr)
        - 'num_children_bioparen': The number of children linked to that parent
    """
    parent_child = (
        pd.concat([
            bioparen_df[["fnr", "pid"]].rename(columns={"fnr": "parent_pid"}),
            bioparen_df[["mnr", "pid"]].rename(columns={"mnr": "parent_pid"}),
        ])
        .dropna(subset=["parent_pid"])
        .astype({"parent_pid": int})
    )

    return (
        parent_child.groupby("parent_pid")
        .size()
        .reset_index(name="num_children_bioparen")
    )


def get_biol_child_counts(biol_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and forward-fill the number of biological children for each respondent over time.

    This function processes panel data with a variable (`lb0285`) that reports the cumulative 
    number of biological children per person. It ensures missing values are forward-filled 
    by person across survey years.

    Parameters:
    - biol_df (pd.DataFrame): A DataFrame containing at least the columns:
        - 'pid': Person identifier
        - 'syear': Survey year
        - 'lb0285': Reported cumulative number of biological children

    Returns:
    - pd.DataFrame: A DataFrame with columns:
        - 'pid': Person identifier
        - 'syear': Survey year
        - 'num_children_biol': Forward-filled number of biological children
    """
    if "lb0285" not in biol_df.columns:
        raise ValueError("biol_df must contain column 'lb0285'")

    df = (
        biol_df[["pid", "syear", "lb0285"]]
        .sort_values(["pid", "syear"])
        .copy()
    )
    df["num_children_biol"] = df.groupby("pid")["lb0285"].ffill()
    return df[["pid", "syear", "num_children_biol"]]
