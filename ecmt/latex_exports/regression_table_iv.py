from pathlib import Path
import joblib
import pandas as pd
import numpy as np

from statsmodels.genmod.generalized_linear_model import GLM
from statsmodels.genmod.families import Binomial
from statsmodels.genmod.families.links import logit as logit_link, probit as probit_link

from ecmt.helpers import load_config

# === CONFIGURATION ===
config = load_config()
models_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
save_dir = Path("/home/alexer/Documents/MScEcon/Semester 2/Master Thesis I/thesis/tables")

# === INPUT & OUTPUT SELECTION ===
logit_filename = "iv_logit_model.pkl"
probit_filename = "iv_probit_model.pkl"
fs_filename = "first_stage_model.pkl"
iv_resid_logit_pkl = models_results_dir / "iv_with_resid_logit.pkl"
iv_resid_probit_pkl = models_results_dir / "iv_with_resid_probit.pkl"


latex_output_filename = "iv_regression_table.tex"

# === Get PKL files ===
logit_pkl = models_results_dir / logit_filename
probit_pkl = models_results_dir / probit_filename
first_stage_pkl = models_results_dir / fs_filename

# === LOAD MODELS FROM PKL ===

result_logit = joblib.load(logit_pkl)
result_probit = joblib.load(probit_pkl)
result_iv_with_resid_logit = joblib.load(iv_resid_logit_pkl)
result_iv_with_resid_probit = joblib.load(iv_resid_probit_pkl)

# === MARGINAL EFFECTS ===
marg_eff_logit = result_logit.get_margeff(at='overall').summary_frame()
marg_eff_probit = result_probit.get_margeff(at='overall').summary_frame()

# === AME COLUMN MAP ===
ame_colmap = {'dy/dx': 'dy/dx', 'se': 'Std. Err.', 'p': 'Pr(>|z|)'}

# === UTILITY FUNCTIONS ===
def compute_pseudo_r2(fitted_model, model_class, link_func):
    y = fitted_model.model.endog
    X_null = pd.DataFrame({"intercept": np.ones(len(y))})
    model_null = model_class(y, X_null, family=Binomial(link=link_func))
    results_null = model_null.fit()
    llf_full = fitted_model.llf
    llf_null = results_null.llf
    return 1 - (llf_full / llf_null)

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



def get_first_stage_stats(pkl_file):
    model = joblib.load(pkl_file)
    r2 = np.round(model.rsquared, 2)
    f_stat = np.round(model.fvalue, 2)
    return r2, f_stat


def extract_dwh_stat(model, resid_var_name="theoretical_bafög_resid"):
    if resid_var_name not in model.params:
        return None, None, None  # residuals not included
    
    coef = model.params[resid_var_name]
    se = model.bse[resid_var_name]
    pval = model.pvalues[resid_var_name]
    return coef, se, pval


# === VARIABLE CATEGORIES ===
categories = {
    "Main explanatory variables": [
        ("joint_income_log", "Parental Income$^\dagger$"),
        ("gross_monthly_income_log", "Student income$^\dagger$"),
        ("theoretical_bafög_hat", "Simulated BAföG amount (IV)")
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

# === R² AND SAMPLE SIZE ===
r2_logit = compute_pseudo_r2(result_logit, GLM, logit_link())
r2_probit = compute_pseudo_r2(result_probit, GLM, probit_link()) 
fs_r2, fs_fstat = get_first_stage_stats(first_stage_pkl)
n_obs = int(result_logit.nobs)

dwh_logit_coef, dwh_logit_se, dwh_logit_pval = extract_dwh_stat(result_iv_with_resid_logit)
dwh_probit_coef, dwh_probit_se, dwh_probit_pval = extract_dwh_stat(result_iv_with_resid_probit)

print(dwh_logit_coef, dwh_logit_pval)
print(dwh_probit_coef, dwh_probit_pval)

# === WRITE LATEX TABLE ===
out_path = save_dir / latex_output_filename
with open(out_path, "w") as f:
    f.write("\\begin{table}\n")
    f.write("\\caption{Instrumental Variable Estimation: $\\Pr(\\mathrm{NTU} = 1 \\mid \\mathbf{X}, \\widehat{\\text{Simulated BAföG}})$}\n")
    f.write("\\renewcommand{\\arraystretch}{1.25}\n")
    f.write("\\footnotesize\n")
    f.write("\\centering\n")
    f.write("\\begin{tabular}{lllllllll}\n")
    f.write("\\toprule\n")
    f.write(" & \\multicolumn{4}{c}{IV Logit} & \\multicolumn{4}{c}{IV Probit} \\\\\n")
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
    f.write("\\midrule\n")
    f.write(f"First-stage $R^2$ & \\multicolumn{{8}}{{l}}{{{fs_r2}}} \\\\\n")
    f.write(f"First-stage F-stat & \\multicolumn{{8}}{{l}}{{{fs_fstat}}} \\\\\n")
    f.write("\\bottomrule\n")
    f.write("\\end{tabular}\n")
    f.write("\\caption*{Logit and Probit Coefficients and Average Marginal Effects}\n")
    f.write("\\label{tab:logit_probit_results}\n")


    notes = (
        f"\\caption*{{\\small{{Notes: Significance levels: $^{{*}} p < 0.1$, $^{{**}} p < 0.05$, $^{{***}} p < 0.01$. "
        f"Robust standard errors clustered at the student level. $^\\dagger$ Indicates log-transformed variables. "
        f"Durbin-Wu-Hausman test residual coef (Logit) = {dwh_logit_coef:.4f} (p = {dwh_logit_pval:.3f}), "
        f"residual coef (Probit) = {dwh_probit_coef:.4f} (p = {dwh_probit_pval:.3f}).}}}}"
    )
    f.write(notes + "\n")

    f.write("\\end{table}\n")

print(f"✅ Exported LaTeX table to {out_path}")
