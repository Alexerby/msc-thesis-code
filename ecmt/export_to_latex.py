from pathlib import Path
import joblib
import pandas as pd

# Load your model results
model_results_dir = Path("~/Downloads/model_results").expanduser()
logit_pkl = model_results_dir / "logit_model.pkl"
probit_pkl = model_results_dir / "probit_model.pkl"

# Load models
result = joblib.load(logit_pkl)
result_probit = joblib.load(probit_pkl)

# Compute AMEs for each
marg_eff_logit = result.get_margeff(at='overall')
marg_eff_probit = result_probit.get_margeff(at='overall')

def add_stars(estimate, pval):
    if pd.isnull(estimate):
        return ""
    if pd.isnull(pval):
        return f"{estimate:.3f}"
    if pval < 0.01:
        return f"{estimate:.3f}***"
    elif pval < 0.05:
        return f"{estimate:.3f}**"
    elif pval < 0.1:
        return f"{estimate:.3f}*"
    else:
        return f"{estimate:.3f}"

def coef_table_with_stars(sm_table, prefix):
    df = sm_table[['Coef.', 'Std.Err.', 'P>|z|']].copy()
    df['Coef.'] = [add_stars(est, p) for est, p in zip(df['Coef.'], df['P>|z|'])]
    df = df[['Coef.', 'Std.Err.']]
    df.columns = pd.MultiIndex.from_product([[prefix], df.columns])
    return df

def ame_table_with_stars(ame, prefix):
    ame = ame.copy()
    if 'P>|z|' in ame.columns:
        pval_col = 'P>|z|'
    elif 'Pr(>|z|)' in ame.columns:
        pval_col = 'Pr(>|z|)'
    else:
        pval_col = None
    if pval_col:
        ame['dy/dx'] = [add_stars(dydx, p) for dydx, p in zip(ame['dy/dx'], ame[pval_col])]
    else:
        ame['dy/dx'] = ame['dy/dx'].map(lambda x: f"{x:.3f}" if pd.notnull(x) else "")
    ame = ame[['dy/dx', 'Std. Err.']]
    ame.columns = pd.MultiIndex.from_product([[prefix], ame.columns])
    return ame

# Build each table with asterisks
logit_coef = coef_table_with_stars(result.summary2().tables[1], "Logit Coef.")
probit_coef = coef_table_with_stars(result_probit.summary2().tables[1], "Probit Coef.")
logit_ame = ame_table_with_stars(marg_eff_logit.summary_frame(), "Logit AME")
probit_ame = ame_table_with_stars(marg_eff_probit.summary_frame(), "Probit AME")

# Align index to ensure all rows/vars match (in case of any mismatch)
all_index = logit_coef.index.union(logit_ame.index).union(probit_coef.index).union(probit_ame.index)
logit_coef = logit_coef.reindex(all_index)
logit_ame = logit_ame.reindex(all_index)
probit_coef = probit_coef.reindex(all_index)
probit_ame = probit_ame.reindex(all_index)

# Concatenate all into one DataFrame
all_df = pd.concat([logit_coef, logit_ame, probit_coef, probit_ame], axis=1)

# --- Force all Std. Err. columns to 3 decimals as strings ---
for col in all_df.columns:
    if col[1] == 'Std. Err.':
        all_df[col] = all_df[col].apply(lambda x: f"{x:.3f}" if pd.notnull(x) and x != '' else "")

# Export to LaTeX
out_tex = model_results_dir / "logit_probit_ame_table.tex"
with open(out_tex, "w") as f:
    f.write(all_df.to_latex(
        na_rep="", multicolumn=True, multirow=True, escape=False,
        caption="Logit/Probit coefficients and AMEs (Average Marginal Effects). Significance: * p<0.1, ** p<0.05, *** p<0.01",
        label="tab:logit-probit-ame"
    ))

print(f"Exported LaTeX table to {out_tex}")
