from pathlib import Path
import joblib
import pandas as pd

# Set save path
model_results_dir = Path("/home/alexer/Documents/MScEcon/Semester 2/Master Thesis I/thesis/tables")

# Load model files
logit_pkl = Path("~/Downloads/model_results/logit_model.pkl").expanduser()
probit_pkl = Path("~/Downloads/model_results/probit_model.pkl").expanduser()

result = joblib.load(logit_pkl)
result_probit = joblib.load(probit_pkl)

# Get marginal effects
marg_eff_logit = result.get_margeff(at='overall').summary_frame()
marg_eff_probit = result_probit.get_margeff(at='overall').summary_frame()

# Column name map
ame_colmap = {'dy/dx': 'dy/dx', 'se': 'Std. Err.', 'p': 'Pr(>|z|)'}

# Significance star helper
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

# Extract row function
def extract_row(name, pretty_name):
    try:
        probit_coef = result_probit.params[name]
        probit_se = result_probit.bse[name]
        probit_pval = result_probit.pvalues[name]
        probit_ame = marg_eff_probit.loc[name, ame_colmap['dy/dx']]
        probit_ame_se = marg_eff_probit.loc[name, ame_colmap['se']]
        probit_ame_pval = marg_eff_probit.loc[name, ame_colmap['p']]

        logit_coef = result.params[name]
        logit_se = result.bse[name]
        logit_pval = result.pvalues[name]
        logit_ame = marg_eff_logit.loc[name, ame_colmap['dy/dx']]
        logit_ame_se = marg_eff_logit.loc[name, ame_colmap['se']]
        logit_ame_pval = marg_eff_logit.loc[name, ame_colmap['p']]

        return f"{pretty_name} & {add_stars(logit_coef, logit_pval)} & {logit_se:.3f} & {add_stars(logit_ame, logit_ame_pval)} & {logit_ame_se:.3f} & {add_stars(probit_coef, probit_pval)} & {probit_se:.3f} & {add_stars(probit_ame, probit_ame_pval)} & {probit_ame_se:.3f} \\\\"
    except KeyError:
        return f"{pretty_name} & & & & & & & & \\\\"

# === Variable categories ===

categories = {
    "Main explanatory variables": [
        ("theoretical_bafög", "Simulated BAföG amount"),
        ("gross_monthly_income_log", "Log Gross income"),
        ("joint_income_log", "Log Parental Income"),
    ],
    "Demographics": [
        ("age", "Age"),
        ("sex", "Female"),
        ("has_partner", "Has partner"),
        ("has_migback", "Migration background"),
    ],
    "Controls": [
        ("lives_at_home", "Living at parents’ home"),
        ("any_sibling_bafog", "Sibling claimed BAföG before"),
        ("east_background", "East background"),
        ("parent_high_edu", "Parents are highly educated"),
    ],
}

# R² and N
r2_logit = result.prsquared
r2_probit = result_probit.prsquared
n_obs = int(result.nobs)

# Start writing LaTeX
out_path = model_results_dir / "regression_table.tex"
with open(out_path, "w") as f:
    f.write("\\begin{table}\n")
    f.write("\\renewcommand{\\arraystretch}{1.25}\n")
    f.write("\\footnotesize\n")
    f.write("\\begin{tabular}{lllllllll}\n")
    f.write("\\toprule\n")
    f.write(" & \\multicolumn{4}{c}{Logit} & \\multicolumn{4}{c}{Probit} \\\\\n")
    f.write("\\cmidrule(lr){2-5} \\cmidrule(lr){6-9}\n")
    f.write(" & Coef. & SE & AME & SE & Coef. & SE & AME & SE \\\\\n")
    f.write("\\midrule\n")

    for category, vars_in_group in categories.items():
        f.write(f"\\multicolumn{{9}}{{l}}{{\\textbf{{{category}}}}} \\\\\n")
        for varname, pretty in vars_in_group:
            f.write(extract_row(varname, pretty) + "\n")
        f.write("\\midrule\n")

    f.write(f"Pseudo $R^2$ & \\multicolumn{{4}}{{l}}{{{r2_logit:.4f}}} & \\multicolumn{{4}}{{l}}{{{r2_probit:.4f}}} \\\\\n")
    f.write(f"Observations & \\multicolumn{{8}}{{l}}{{{n_obs}}} \\\\\n")
    f.write("\\bottomrule\n")
    f.write("\\end{tabular}\n")
    f.write("\\caption{Logit/Probit coefficients and AMEs (Average Marginal Effects). Significance: $^{{*}} p < 0.1$, $^{{**}} p < 0.05$, $^{{***}} p < 0.01$. Robust standard errors are clustered on student level.}\n")
    f.write("\\end{table}\n")

print(f"Exported LaTeX table to {out_path}")
