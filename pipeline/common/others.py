import pandas as pd


def add_weight_col(df: pd.DataFrame, phrf_df: pd.DataFrame) -> pd.DataFrame:

    WEIGHT_COL_MAP = {
        1984: "aphrf", 1985: "bphrf", 1986: "cphrf", 1987: "dphrf", 1988: "ephrf",
        1989: "fphrf", 1990: "gphrf", 1991: "hphrf", 1992: "iphrf", 1993: "jphrf",
        1994: "kphrf", 1995: "lphrf", 1996: "mphrf", 1997: "nphrf", 1998: "ophrf",
        1999: "pphrf", 2000: "qphrf", 2001: "rphrf", 2002: "sphrf", 2003: "tphrf",
        2004: "uphrf", 2005: "vphrf", 2006: "wphrf", 2007: "xphrf", 2008: "yphrf",
        2009: "zphrf", 2010: "baphrf", 2011: "bbphrf", 2012: "bcphrf", 2013: "bdphrf",
        2014: "bephrf", 2015: "bfphrf", 2016: "bgphrf", 2017: "bhphrf", 2018: "biphrf",
        2019: "bjphrf", 2020: "bkphrf", 2021: "blphrf",
    }

    # Merge in all columns from phrf_df (wide, by pid)
    phrf_cols = ["pid"] + list(WEIGHT_COL_MAP.values())
    phrf_df = phrf_df[phrf_cols].copy()
    merged = df.merge(phrf_df, on="pid", how="left")
    
    # Pick correct weight for each row based on syear
    def select_weight(row):
        col = WEIGHT_COL_MAP.get(row["syear"])
        return row.get(col, pd.NA)

    merged["phrf"] = merged.apply(select_weight, axis=1)
    
    # Optional: Drop the many weight columns, keep just ["pid", "syear", ... original ..., "phrf"]
    drop_cols = [c for c in WEIGHT_COL_MAP.values()]
    merged = merged.drop(columns=drop_cols)
    return merged

