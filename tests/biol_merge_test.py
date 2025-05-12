
import pandas as pd

def test_merge_on_pid_syear(df: pd.DataFrame, biol_df: pd.DataFrame):
    """
    Test whether merging biol_df onto df by ["pid", "syear"] is valid
    (i.e., does not duplicate rows in df).
    """
    # Check uniqueness of merge keys in biol_df
    duplicates = biol_df.duplicated(["pid", "syear"]).sum()
    assert duplicates == 0, f"biol_df is not unique on ['pid', 'syear']: found {duplicates} duplicates"

    # Check row count stability after merge
    merged = df.merge(biol_df, on=["pid", "syear"], how="left")
    assert len(merged) == len(df), (
        f"Merge resulted in row duplication: "
        f"{len(merged)} rows after merge vs {len(df)} before"
    )

    print("✅ Merge is valid: no duplicates introduced.")
