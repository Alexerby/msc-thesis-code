from pathlib import Path
import pandas as pd
from statsmodels.discrete.discrete_model import Probit
import statsmodels.api as sm
from ecmt.parquet_loader import BafoegParquetLoader

# 1. Load your parquet file
parquet_path = "~/Downloads/BAföG Results/parquets/bafoeg_calculations.parquet"
loader = BafoegParquetLoader(parquet_path)

# 2. Get the DataFrame
df = loader.get()

# 3. (Optional) Inspect the data
print(df[["received_bafög", "theoretical_bafög"]].head())

# 4. Prepare the data for Probit
df_model = df[["received_bafög", "theoretical_bafög"]].dropna()

y = df_model["received_bafög"]
X = df_model[["theoretical_bafög"]]
X = sm.add_constant(X)

# 5. Fit Probit model
probit_mod = Probit(y, X).fit()
print(probit_mod.summary())

# 6. Compute McFadden's pseudo R²
llf = probit_mod.llf
llnull = probit_mod.llnull
mcFadden_r2 = 1 - (llf / llnull)
print(f"McFadden's pseudo R^2: {mcFadden_r2:.3f}")
