import os
import pandas as pd
import matplotlib.pyplot as plt

from pipeline.build import BafoegPipeline
from misc.utility_functions import load_project_config
from loaders.registry import LoaderRegistry
from descriptives.helpers import load_data


# Load your data
df = load_data("bafoeg_calculations", from_parquet=True)

# Prepare binary indicators
df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
df["R"] = df["received_bafög"].fillna(0).astype(int)
df = df.dropna(subset=["syear"])

# Compute Pr(R = 0 | M = 1) for each year (i.e., NTU rate)
results = []
for syear, group in df.groupby("syear"):
    m1 = group[group["M"] == 1]
    total = len(m1)
    if total > 0:
        p_ntu = (m1["R"] == 0).mean()
        se = (p_ntu * (1 - p_ntu) / total) ** 0.5  # Binomial SE
    else:
        p_ntu = pd.NA
        se = pd.NA

    results.append({
        "syear": syear,
        "lower_bound": p_ntu - se,
        "upper_bound": p_ntu + se,
        "mean_ntu": p_ntu,
        "se": se
    })

ntu_df = pd.DataFrame(results).sort_values("syear")

# Clip values to stay in [0, 1]
ntu_df["lower_bound"] = ntu_df["lower_bound"].clip(0, 1)
ntu_df["upper_bound"] = ntu_df["upper_bound"].clip(0, 1)

# Plot
fig, ax = plt.subplots(figsize=(9, 5))

# Plot the point estimate
ax.plot(ntu_df["syear"], ntu_df["mean_ntu"], 'o-', color='black', label='Mean NTU')


# Plot lower and upper bounds with dashed lines
ax.plot(ntu_df["syear"], ntu_df["lower_bound"], '--', color='gray', label='Lower bound')
ax.plot(ntu_df["syear"], ntu_df["upper_bound"], '--', color='darkgray', label='Upper bound')

# Axis labels and formatting
ax.set_xlabel("Year")
ax.set_ylabel("Pr(NTU)")
ax.set_ylim(0.3, 0.8)
ax.set_xticks(ntu_df["syear"])
ax.grid(True, axis='y', linestyle='--', alpha=0.6)
ax.legend(loc='lower center', frameon=False, ncol=3)

plt.tight_layout()
plt.show()
