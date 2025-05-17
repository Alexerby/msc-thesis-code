import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.formula.api import ols
from statsmodels.stats.diagnostic import het_breuschpagan
from ecmt.parquet_loader import BafoegParquetLoader

# === 1. Load data ===
loader = BafoegParquetLoader("~/Downloads/BAföG Results/parquets/bafoeg_calculations.parquet")
df = loader.get()

# Filter to cases with positive theoretical BAföG
ols_df = df[df["theoretical_bafög"] > 0].copy()

# === 2. Fit baseline OLS model ===
formula = "theoretical_bafög ~ excess_income_par + excess_income_stu + excess_income_assets"
ols_model = ols(formula=formula, data=ols_df).fit()

print("=== OLS Summary ===")
print(ols_model.summary())

# === 4. Breusch-Pagan Test for Heteroskedasticity ===
fitted_vals = ols_model.fittedvalues
residuals = ols_model.resid

exog = ols_model.model.exog  # includes intercept
bp_stat, bp_pval, f_stat, f_pval = het_breuschpagan(residuals, exog)

print("\n=== Breusch-Pagan Test ===")
print(f"LM statistic: {bp_stat:.2f}")
print(f"p-value: {bp_pval:.4f}")

# === 5. Fit model with robust (heteroskedasticity-consistent) standard errors ===
robust_model = ols(formula=formula, data=ols_df).fit(cov_type='HC1')

print("\n=== OLS with Robust Standard Errors (HC1) ===")
print(robust_model.summary())
