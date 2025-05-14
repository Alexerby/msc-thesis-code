
import pandas as pd

def add_age(df: pd.DataFrame, ppath_df: pd.DataFrame) -> pd.DataFrame:
    p = ppath_df[["pid", "syear", "gebjahr", "gebmonat"]].copy()

    # remove implausible years (e.g. gebjahr < 1900 or missing)
    p = p[p["gebjahr"].between(1900, 2025)]  # adjust upper bound as needed

    p["age"] = p["syear"] - p["gebjahr"] - (p["gebmonat"] > 6).astype(int)

    return (
        df.drop(columns=["gebjahr", "gebmonat"], errors="ignore")
          .merge(p[["pid", "syear", "age"]], on=["pid", "syear"], how="left")
    )
