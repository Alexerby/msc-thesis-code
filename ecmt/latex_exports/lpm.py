from pathlib import Path
import joblib
import pandas as pd
import numpy as np
from misc.utility_functions import load_project_config

# === CONFIGURATION ===
config = load_project_config()
models_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
save_dir = Path("/home/alexer/Documents/MScEcon/Semester 2/Master Thesis I/thesis/tables")

lpm_filename = "lpm_model.pkl"
latex_output_filename = "lpm_regression_table.tex"

lpm_pkl = models_results_dir / lpm_filename
result_lpm = joblib.load(lpm_pkl)

# === UTILITY FUNCTIONS ===
def add_stars(est, pval):
    if pd.isnull(est) or pd.isnull(pval):
        return f"{est:.3f}" if pd.notnull(est) else ""
    if pval < 0.01:
        return f"{est:.3f}***"
    elif pval < 0.05:
        return f"{est:.3f}**"
    elif pval < 0.1:
        return f"{est:.3f}*"
    else:
        return f"{est:.3f}"

def extract_row(name, pretty_name):
    try:
        coef = result_lpm.params[name]
        se = result_lpm.bse[name]
        pval = result_lpm.pvalues[name]
        return f"{pretty_name} & {add_stars(coef, pval)} & {se:.3f} \\\\"
    except KeyError:
        return f"{pretty_name} & & \\\\"

# === VARIABLE CATEGORIES (same as above, update as needed) ===
categories = {
    "Main explanatory variables": [
        ("theoretical_bafög", "Simulated BAföG amount$^{\circ}$"),
    ],
    "Controls: Demographics": [
        ("age", "Age"),
        ("sex_2", "Female"),
        ("has_partner_1", "Has partner"),
        ("migback_2", "Direct Migration background"),
        ("migback_3", "Indirect Migration background"),
    ],
    "Controls: Household and Socioeconomic Background": [
        ("lives_at_home_1", "Living at parents’ home"),
        ("any_sibling_bafog_1_0", "Sibling claimed BAföG before"),
        ("east_background", "East background"),
        ("parent_high_edu", "Parents are highly educated"),
    ],
    "Controls: Behaviour": [
        ("plh0253", "Patience"),
        ("plh0254", "Impulsiveness"),
        ("plh0204_h", "Risk Appetite"),
    ]
}

# === R², F-STATISTIC, N ===
r2 = result_lpm.rsquared
fstat = result_lpm.fvalue
fstat_p = result_lpm.f_pvalue
n_obs = int(result_lpm.nobs)

# === WRITE LATEX TABLE ===
out_path = save_dir / latex_output_filename
with open(out_path, "w") as f:
    f.write("\\begin{table}\n")
    f.write("\\caption{Linear Probability Model: $\\Pr(\\mathrm{NTU} = 1 \\mid \\mathbf{X})$}\n")
    f.write("\\renewcommand{\\arraystretch}{1.25}\n")
    f.write("\\footnotesize\n")
    f.write("\\centering\n")
    f.write("\\begin{tabular}{lccc}\n")
    f.write("\\toprule\n")
    f.write(" & Coef. & SE \\\\\n")
    f.write("\\midrule\n")

    for category, vars_in_group in categories.items():
        f.write(f"\\multicolumn{{4}}{{l}}{{\\textbf{{{category}}}}} \\\\\n")
        for varname, pretty in vars_in_group:
            f.write(extract_row(varname, pretty) + "\n")
        f.write("\\midrule\n")

    f.write(f"$R^2$ & \\multicolumn{{3}}{{l}}{{{r2:.2f}}} \\\\\n")
    f.write(f"F-statistic & \\multicolumn{{3}}{{l}}{{{fstat:.2f} (p = {fstat_p:.2f})}} \\\\\n")
    f.write(f"Observations & \\multicolumn{{3}}{{l}}{{{n_obs}}} \\\\\n")
    f.write("\\bottomrule\n")
    f.write("\\end{tabular}\n")
    f.write("\\caption*{Linear Probability Model (OLS) Coefficients. Robust standard errors clustered at the student level.}\n")
    f.write("\\label{tab:lpm_results}\n")
    f.write("\\caption*{\\small{Notes: Significance levels: $^{{*}} p < 0.1$, $^{{**}} p < 0.05$, $^{{***}} p < 0.01$. $^\\circ$ Indicates per 100 EUR.}}\n")
    f.write("\\end{table}\n")

print(f"✅ Exported LPM LaTeX table to {out_path}")
