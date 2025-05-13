import pandas as pd 
import numpy as np

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.common.processor_income import merge_income

def add_income(df: pd.DataFrame, data: SOEPDataBundle) -> pd.DataFrame:
    return (
        df.copy()
        .pipe(merge_income, data.pkal)
    )

