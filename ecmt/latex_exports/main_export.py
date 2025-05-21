from pathlib import Path
import joblib
import pandas as pd
import numpy as np
from scipy.stats import chi2

from statsmodels.genmod.generalized_linear_model import GLM
from statsmodels.genmod.families import Binomial
from statsmodels.genmod.families.links import Logit, Probit  # <- Updated import!
from misc.utility_functions import load_project_config

# === CONFIGURATION ===
config = load_project_config()
models_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
save_dir = Path("/home/alexer/Documents/MScEcon/Semester 2/Master Thesis I/thesis/tables")

# === INPUT & OUTPUT SELECTION ===
logit_filename = "logit_model.pkl"
probit_filename = "probit_model.pkl"
lpm_filename = "lpm_model.pkl"
latex_output_filename = "regression_table.tex"

# === LOAD MODELS ===
logit_pkl = models_results_dir / logit_filename
probit_pkl = models_results_dir / probit_filename
lpm_pkl = models_results_dir / lpm_filename

result_logit = joblib.load(logit_pkl)
result_probit = joblib.load(probit_pkl)
result_lpm = joblib.load(lpm_pkl)

# === MARGINAL EFFECTS ===
marg_eff_logit = result_logit.get_margeff(at='overall').summary_frame()
marg_eff_probit = result_probit.get_margeff(at='overall').summary_frame()

# === AME COLUMN MAP ===
ame_colmap = {'dy/dx': 'dy/dx', 'se': 'Std. Err.', 'p': 'Pr(>|z|)'}

# === UTILITY FUNCTIONS ===
def compute_mcfadden_pseudo_r2(fitted_model, model_class, link_func):
    y = fitted_model.model.endog
    X_null = pd.DataFrame({"intercept": np.ones(len(y))})
    model_null = model_class(y, X_null, family=Binomial(link=link_func))
    results_null = model_null.fit()
    llf_full = fitted_model.llf
    llf_null = results_null.llf
    return 1 - (llf_full / llf_null)

def compute_cox_snell_r2(fitted_model):
    llf = fitted_model.llf
    llnull = fitted_model.llnull
    n = fitted_model.nobs
    return 1 - np.exp((2 / n) * (llnull - llf))

def compute_nagelkerke_r2(fitted_model):
    cs_r2 = compute_cox_snell_r2(fitted_model)
    llnull = fitted_model.llnull
    n = fitted_model.nobs
    max_r2 = 1 - np.exp((2 / n) * llnull)
    return cs_r2 / max_r2 if max_r2 != 0 else np.nan

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

def extract_row(name):
    # First row: coefficients, second row: SEs (as strings, empty if missing)
    try:
        logit_coef, logit_se, logit_pval = (
            result_logit.params[name], result_logit.bse[name], result_logit.pvalues[name]
        )
        logit_coef_str = add_stars(logit_coef, logit_pval)
        logit_se_str = f"({logit_se:.3f})"
        logit_ame = marg_eff_logit.loc[name, ame_colmap['dy/dx']]
        logit_ame_se = marg_eff_logit.loc[name, ame_colmap['se']]
        logit_ame_pval = marg_eff_logit.loc[name, ame_colmap['p']]
        logit_ame_str = add_stars(logit_ame, logit_ame_pval)
        logit_ame_se_str = f"({logit_ame_se:.3f})"
    except Exception:
        logit_coef_str = logit_se_str = logit_ame_str = logit_ame_se_str = ""

    try:
        probit_coef, probit_se, probit_pval = (
            result_probit.params[name], result_probit.bse[name], result_probit.pvalues[name]
        )
        probit_coef_str = add_stars(probit_coef, probit_pval)
        probit_se_str = f"({probit_se:.3f})"
        probit_ame = marg_eff_probit.loc[name, ame_colmap['dy/dx']]
        probit_ame_se = marg_eff_probit.loc[name, ame_colmap['se']]
        probit_ame_pval = marg_eff_probit.loc[name, ame_colmap['p']]
        probit_ame_str = add_stars(probit_ame, probit_ame_pval)
        probit_ame_se_str = f"({probit_ame_se:.3f})"
    except Exception:
        probit_coef_str = probit_se_str = probit_ame_str = probit_ame_se_str = ""

    try:
        lpm_coef = result_lpm.params[name]
        lpm_se = result_lpm.bse[name]
        lpm_pval = result_lpm.pvalues[name]
        lpm_coef_str = add_stars(lpm_coef, lpm_pval)
        lpm_se_str = f"({lpm_se:.3f})"
    except Exception:
        lpm_coef_str = lpm_se_str = ""

    # Point estimate row (1st):   logit_coef, logit_ame, probit_coef, probit_ame, lpm_coef
    est_row = f"{logit_coef_str} & {logit_ame_str} & {probit_coef_str} & {probit_ame_str} & {lpm_coef_str} \\\\"
    # SE row (2nd):               logit_se, logit_ame_se, probit_se, probit_ame_se, lpm_se
    se_row  = f"{logit_se_str} & {logit_ame_se_str} & {probit_se_str} & {probit_ame_se_str} & {lpm_se_str} \\\\"
    return est_row, se_row

# === VARIABLE CATEGORIES ===
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
        ("plh0204_h", "Risk Apetite"),
    ]
}

def compute_lr_test(fitted_model):
    llf_full = fitted_model.llf
    llf_null = fitted_model.llnull
    df_diff = fitted_model.df_model  # excludes intercept
    lr_stat = -2 * (llf_null - llf_full)
    p_value = chi2.sf(lr_stat, df_diff)
    return lr_stat, p_value

lr_stat_logit, lr_pval_logit = compute_lr_test(result_logit)
lr_stat_probit, lr_pval_probit = compute_lr_test(result_probit)

# === R² AND SAMPLE SIZE ===
r2_mcfadden_logit = compute_mcfadden_pseudo_r2(result_logit, GLM, Logit())
r2_mcfadden_probit = compute_mcfadden_pseudo_r2(result_probit, GLM, Probit())

r2_cs_logit = compute_cox_snell_r2(result_logit)
r2_cs_probit = compute_cox_snell_r2(result_probit)

r2_nagelkerke_logit = compute_nagelkerke_r2(result_logit)
r2_nagelkerke_probit = compute_nagelkerke_r2(result_probit)

n_obs = int(result_logit.nobs)
r2_lpm_adj = result_lpm.rsquared_adj
f_stat = result_lpm.fvalue
f_pval = result_lpm.f_pvalue

# === WRITE LATEX TABLE ===
out_path = save_dir / latex_output_filename
with open(out_path, "w") as f:
    f.write("\\begin{table}\n")
    f.write("\\caption{$\\Pr(\\mathrm{NTU} = 1 \\mid \\mathbf{X})$}\n")
    f.write("\\renewcommand{\\arraystretch}{1.25}\n")
    f.write("\\centering\n")
    f.write("\\begin{tabular}{lccccc}\n")
    f.write("\\toprule\n")
    f.write("& \\multicolumn{2}{c}{Logit} & \\multicolumn{2}{c}{Probit} & LPM \\\\\n")
    f.write("& Coef. & AME & Coef. & AME & Coef. \\\\\n")
    f.write("\\midrule\n")

    for category, vars_in_group in categories.items():
        f.write(f"\\multicolumn{{6}}{{l}}{{\\textbf{{{category}}}}} \\\\\n")
        for varname, pretty in vars_in_group:
            est_row, se_row = extract_row(varname)
            # Display pretty name with note symbol if needed
            display_name = pretty
            if varname == "simulated_bafög_amount":
                display_name += " $^\\dagger$"
            f.write(f"{display_name} & {est_row}\n")
            f.write(f" & {se_row}\n")
        f.write("\\midrule\n")

    # Summary stats: one column for label, rest filled to align
    f.write(f"McFadden Pseudo $R^2$ & \\multicolumn{{2}}{{l}}{{{r2_mcfadden_logit:.2f}}} & \\multicolumn{{2}}{{l}}{{{r2_mcfadden_probit:.2f}}} & \\\\\n")
    f.write(f"Cox and Snell Pseudo $R^2$ & \\multicolumn{{2}}{{l}}{{{r2_cs_logit:.2f}}} & \\multicolumn{{2}}{{l}}{{{r2_cs_probit:.2f}}} & \\\\\n")
    f.write(f"Nagelkerke Pseudo $R^2$ & \\multicolumn{{2}}{{l}}{{{r2_nagelkerke_logit:.2f}}} & \\multicolumn{{2}}{{l}}{{{r2_nagelkerke_probit:.2f}}} & \\\\\n")
    f.write(f"Likelihood Ratio Test & \\multicolumn{{2}}{{l}}{{{lr_stat_logit:.2f} (p = {lr_pval_logit:.2f})}} & \\multicolumn{{2}}{{l}}{{{lr_stat_probit:.2f} (p = {lr_pval_probit:.2f})}} & \\\\\n")
    f.write(f"Adjusted $R^2$ & & & & & {r2_lpm_adj:.2f} \\\\\n")
    f.write(f"F-statistic & & & & & {f_stat:.1f} (p = {f_pval:.2f}) \\\\\n")
    f.write(f"Observations & \\multicolumn{{5}}{{l}}{{{n_obs}}} \\\\\n")
    f.write("\\bottomrule\n")
    f.write("\\end{tabular}\n")
    f.write("\\caption*{Logit, Probit, and LPM (Linear Probability Model) coefficients. Logit and Probit also report average marginal effects. Standard errors are in parentheses. The LPM is estimated via OLS with MacKinnon and White (1985) robust (HC3) standard errors.}\n")
    f.write("\\label{tab:logit_probit_lpm_results}\n")
    f.write("\\caption*{\\small{Notes: Significance levels: $^{{*}} p < 0.1$, $^{{**}} p < 0.05$, $^{{***}} p < 0.01$. Robust standard errors clustered at the student level. $\circ$ Indicates per 100 EUR.}}\n")
    f.write("\\end{table}\n")

print(f"✅ Exported LaTeX table to {out_path}")
