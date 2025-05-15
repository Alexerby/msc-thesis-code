import pandas as pd


def assert_no_reported_bafog_inconsistency(df: pd.DataFrame) -> None:
    """
    Checks for inconsistencies where individuals are marked as receiving BAföG
    (received_bafög = 1) but not reporting it (reported_bafög = 0).  Raises an
    AssertionError if any such inconsistencies are found.  Does not modify
    the input DataFrame.

    Args:
        df: The pandas DataFrame to check.  It is assumed to contain the
            columns 'received_bafög' and 'reported_bafög'.

    Returns:
        None.  If no inconsistencies are found, the function returns silently.

    Raises:
        AssertionError: If any rows have received_bafög = 1 and reported_bafög = 0.
    """
    inconsistent_cases = df[(df['received_bafög'] == 1) & (df['reported_bafög'] == 0)]
    num_inconsistent = len(inconsistent_cases)

    if num_inconsistent > 0:
        error_message = (
            f"Found {num_inconsistent} cases where received_bafög is 1 but "
            f"reported_bafög is 0.  The first few are:\n"
            f"{inconsistent_cases.head().to_string()}"  # Show a few examples
        )
        raise AssertionError(error_message)
    else:
        print("Sanity check passed: No inconsistencies found between received_bafög and reported_bafög.")

