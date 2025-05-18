import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib
from pathlib import Path

# Import your config helper (adapt path if needed)
from ecmt.helpers import load_config

# === Load config and get model path ===
config = load_config()
model_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()

# Choose which model you want to plot: logit or probit
MODEL_TYPE = "logit"  # or "probit"
model_filename = f"{MODEL_TYPE}_model.pkl"
model_path = model_results_dir / model_filename

# === Load your processed data ===
df = pd.read_parquet("~/Downloads/BAföG Results/parquets/bafoeg_calculations.parquet")

# === Load your fitted model ===
result = joblib.load(model_path)

# === Set up the variable for plotting ===
X_plot = np.linspace(0, 6, 13)  # 0, 0.5, ..., 6
key_var = "simulated_bafog_amt_100"  # CHANGE if your column is named differently

# === Prepare other variables at their mean/mode (to isolate effect of key_var) ===
model_vars = [v for v in result.model.exog_names if v != 'Intercept' and v != key_var]

pred_df = pd.DataFrame({key_var: X_plot})
for v in model_vars:
    if df[v].dtype.kind in 'biufc':
        pred_df[v] = df[v].mean()
    else:
        pred_df[v] = df[v].mode()[0]

# === Predict probability and CI ===
pred = result.get_prediction(pred_df)
pred_mean = pred.predicted_mean
conf_int = pred.conf_int()
lower = conf_int[:, 0]
upper = conf_int[:, 1]

# === Plotting ===
plt.figure(figsize=(6, 4))
plt.errorbar(X_plot, pred_mean, yerr=[pred_mean - lower, upper - pred_mean],
             fmt='o-', color='k', ecolor='gray', capsize=4, label="Predicted Pr(NTU)")

plt.xlabel('Simulated BAföG amount, defl., in 100 EUR')
plt.ylabel('Pr(NTU)')
plt.ylim(0, 1)
plt.title('Predicted Non-Take-Up vs BAföG Amount')
plt.legend()
plt.tight_layout()
plt.show()
