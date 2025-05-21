import pandas as pd
import numpy as np
import patsy
import joblib
from pathlib import Path
from misc.utility_functions import load_project_config
from statsmodels.genmod.generalized_linear_model import GLM
from statsmodels.genmod.families import Binomial
from statsmodels.genmod.families.links import logit as logit_link, probit as probit_link

def create_dummy(df, varname):
    dummies = pd.get_dummies(df[varname], prefix=varname, drop_first=True)
    df = df.drop(columns=[varname])  # Important: drop original
    df = pd.concat([df, dummies], axis=1)
    return df, list(dummies.columns)

def run_binary_model(df, formula, model_type="logit", cluster_var=None):
    """
    Run a GLM binary choice model (logit or probit) with given formula.
    If cluster_var is given, use cluster robust standard errors on that variable.
    """
    y, X = patsy.dmatrices(formula, data=df, return_type='dataframe')

    link_func = logit_link() if model_type == "logit" else probit_link()
    model = GLM(y, X, family=Binomial(link=link_func))

    fit_kwargs = {}
    if cluster_var is not None:
        groups = df.loc[X.index, cluster_var]
        fit_kwargs["cov_type"] = "cluster"
        fit_kwargs["cov_kwds"] = {"groups": groups}

    results = model.fit(**fit_kwargs)

    print(f"=== {model_type.capitalize()} Model Results ===")
    print(results.summary())
    return results

if __name__ == "__main__":
    from .data_loader import load_and_merge

    df = load_and_merge()
    print(df.head())

    # Subset to eligible students only
    df = df[df["theoretical_eligibility"] == 1].copy()

    # Prepare binary outcome variable (non_take_up_obs to int)
    df["non_take_up"] = df["non_take_up_obs"].astype(int)

    df["age_sq"] = df["age"] ** 2

    # Scale incomes by 000 for per 000 EUR interpretation, safely convert to numeric
    df["gross_monthly_income"] = pd.to_numeric(df["gross_monthly_income"], errors='coerce') / 100
    df["joint_income"] = pd.to_numeric(df["joint_income"], errors='coerce') / 100
    df["theoretical_bafög"] = pd.to_numeric(df["theoretical_bafög"], errors='coerce') / 100

    # Log transform parental and gross monthly income, only for positive values; else NaN
    df["log_gross_monthly_income"] = np.where(
        df["gross_monthly_income"] > 0,
        np.log(df["gross_monthly_income"]),
        np.nan
    )
    df["log_joint_income"] = np.where(
        df["joint_income"] > 0,
        np.log(df["joint_income"]),
        np.nan
    )

    # Create dummies for categorical variables to avoid dummy trap
    df, sex_dummies = create_dummy(df, "sex")
    df, partner_dummies = create_dummy(df, "has_partner")
    df, migback_dummies = create_dummy(df, "migback")  # here we do migback with dummy function!

    # Convert dummy columns to int explicitly
    for col in sex_dummies + partner_dummies + migback_dummies:
        df[col] = df[col].astype(int)

    # List of model variables including logged incomes + generated dummies
    vars = [
        "log_joint_income",
        "log_gross_monthly_income",
        "theoretical_bafög",
        "age",
        "lives_at_home",
        "parent_high_edu",
        "any_sibling_bafog",
        "east_background",
    ] + sex_dummies + partner_dummies + migback_dummies

    formula = "non_take_up ~ " + " + ".join(vars) + " - 1"

    # Run Logit with clustering on pid
    logit_results = run_binary_model(df, formula, model_type="logit", cluster_var="pid")

    # Run Probit with clustering on pid
    probit_results = run_binary_model(df, formula, model_type="probit", cluster_var="pid")

    # Save models
    config = load_project_config()
    model_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
    model_results_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(logit_results, model_results_dir / "logit_model.pkl")
    joblib.dump(probit_results, model_results_dir / "probit_model.pkl")

    print(f"Models saved to {model_results_dir}")
