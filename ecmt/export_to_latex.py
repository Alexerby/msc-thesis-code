from pathlib import Path
import joblib
import pandas as pd
import numpy as np 

from statsmodels.genmod.generalized_linear_model import GLM
from statsmodels.genmod.families import Binomial
from statsmodels.genmod.families.links import logit as logit_link, probit as probit_link

from ecmt.helpers import load_config

config = load_config()
models_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()

# Set save path
save_dir = Path("/home/alexer/Documents/MScEcon/Semester 2/Master Thesis I/thesis/tables")

# Load model files
logit_pkl = Path(models_results_dir / "logit_model.pkl")
probit_pkl = Path(models_results_dir / "probit_model.pkl")


result_logit = joblib.load(logit_pkl)
result_probit = joblib.load(probit_pkl)

# Get marginal effects
marg_eff_logit = result_logit.get_margeff(at='overall').summary_frame()
marg_eff_probit = result_probit.get_margeff(at='overall').summary_frame()

# Column name map
ame_colmap = {'dy/dx': 'dy/dx', 'se': 'Std. Err.', 'p': 'Pr(>|z|)'}


# === Utility functions ===
def compute_pseudo_r2(fitted_model, model_class, link_func):
    y = fitted_model.model.endog
    X_null = pd.DataFrame({"intercept": np.ones(len(y))})
    model_null = model_class(y, X_null, family=Binomial(link=link_func))
    results_null = model_null.fit()
    llf_full = fitted_model.llf
    llf_null = results_null.llf
    return 1 - (llf_full / llf_null)

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

        logit_coef = result_logit.params[name]
        logit_se = result_logit.bse[name]
        logit_pval = result_logit.pvalues[name]
        logit_ame = marg_eff_logit.loc[name, ame_colmap['dy/dx']]
        logit_ame_se = marg_eff_logit.loc[name, ame_colmap['se']]
        logit_ame_pval = marg_eff_logit.loc[name, ame_colmap['p']]

        return f"{pretty_name} & {add_stars(logit_coef, logit_pval)} & {logit_se:.3f} & {add_stars(logit_ame, logit_ame_pval)} & {logit_ame_se:.3f} & {add_stars(probit_coef, probit_pval)} & {probit_se:.3f} & {add_stars(probit_ame, probit_ame_pval)} & {probit_ame_se:.3f} \\\\"
    except KeyError:
        return f"{pretty_name} & & & & & & & & \\\\"

# === Variable categories ===

categories = {
    "Main explanatory variables": [
        ("joint_income_log", "Parental Income$^\dagger$"),
        ("gross_monthly_income_log", "Student income$^\dagger$"),
        ("theoretical_bafög", "Simulated BAföG amount"),
    ],
    "Demographics": [
        ("age", "Age"),
        ("age_sq", "Age sq"),
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


# === R² and observations ===
r2_logit = compute_pseudo_r2(result_logit, GLM, logit_link())
r2_probit = compute_pseudo_r2(result_probit, GLM, probit_link())
n_obs = int(result_logit.nobs)

# Start writing LaTeX
out_path = save_dir / "regression_table.tex"
with open(out_path, "w") as f:
    f.write("\\begin{table}\n")
    f.write("\\caption{$\Pr(\mathrm{NTU} = 1 | \mathbf{X})$}\n")
    f.write("\\renewcommand{\\arraystretch}{1.25}\n")
    f.write("\\footnotesize\n")
    f.write("\\centering\n") 
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
    f.write("\\caption*{Logit and Probit Coefficients and Average Marginal Effects}\n")
    f.write("\\label{tab:logit_probit_results}\n")
    f.write("\\caption*{\small{Notes: Significance levels indicated by $^{{*}} p < 0.1$, $^{{**}} p < 0.05$, $^{{***}} p < 0.01$. Robust standard errors clustered at the student level. $^\dagger$ Indicates that the variable has been log-transformed.}}")
    f.write("\\end{table}\n")

print(f"Exported LaTeX table to {out_path}")
