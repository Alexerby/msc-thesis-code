import pandas as pd 


def get_bioparen_child_counts(bioparen_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame with `parent_pid` and number of known biological children.
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
    Returns biol_df with forward-filled child counts (num_children_biol) per pid.
    Assumes biol_df has ['pid', 'syear', 'lb0285'].
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
