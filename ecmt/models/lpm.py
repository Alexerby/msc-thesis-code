import pandas as pd
import numpy as np
import patsy
import joblib
from pathlib import Path
from misc.utility_functions import load_project_config
import statsmodels.api as sm

from .helpers import create_dummy, sanitize_column_names


def run_linear_probability_model(df, formula, cluster_var=None):
    y, X = patsy.dmatrices(formula, data=df, return_type='dataframe')
    model = sm.OLS(y, X)

    fit_kwargs = {"cov_type": "HC3"}
    if cluster_var is not None:
        groups = df.loc[X.index, cluster_var]
        fit_kwargs["cov_type"] = "cluster"
        fit_kwargs["cov_kwds"] = {"groups": groups}

    results = model.fit(**fit_kwargs)
    print("=== Linear Probability Model (OLS) Results ===")
    print(results.summary())
    return results

if __name__ == "__main__":
    from .data_loader import load_and_merge

    df = load_and_merge()
    print(df.head())

    df = df[df["theoretical_eligibility"] == 1].copy()
    df["non_take_up"] = df["non_take_up_obs"].astype(int)
    df["age_sq"] = df["age"] ** 2

    df["gross_monthly_income"] = pd.to_numeric(df["gross_monthly_income"], errors='coerce') / 100
    df["joint_income"] = pd.to_numeric(df["joint_income"], errors='coerce') / 100
    df["theoretical_bafög"] = pd.to_numeric(df["theoretical_bafög"], errors='coerce') / 100

    df = df[(df["gross_monthly_income"] > 0) & (df["joint_income"] > 0)].copy()

    epsilon = 0.01
    df["log_gross_monthly_income"] = np.log(df["gross_monthly_income"] + epsilon)
    df["log_joint_income"] = np.log(df["joint_income"] + epsilon)

    df, sex_dummies = create_dummy(df, "sex")
    df, sex_dummies = sanitize_column_names(df, sex_dummies)

    df, partner_dummies = create_dummy(df, "has_partner")
    df, partner_dummies = sanitize_column_names(df, partner_dummies)

    df, migback_dummies = create_dummy(df, "migback")
    df, migback_dummies = sanitize_column_names(df, migback_dummies)

    df["any_sibling_bafog"] = df["any_sibling_bafog"].astype(str)
    df, any_sibling_bafog_dummies = create_dummy(df, "any_sibling_bafog")
    df, any_sibling_bafog_dummies = sanitize_column_names(df, any_sibling_bafog_dummies)

    df, lives_at_home_dummies = create_dummy(df, "lives_at_home")
    df, lives_at_home_dummies = sanitize_column_names(df, lives_at_home_dummies)

    df["patience_impulsive_risk"] = (
        df["plh0253"] * df["plh0254"] * df["plh0204_h"]
    )

    if 'any_sibling_bafog_nan' in df.columns:
        df.drop(columns=['any_sibling_bafog_nan'], inplace=True)
        any_sibling_bafog_dummies = [col for col in any_sibling_bafog_dummies if col != 'any_sibling_bafog_nan']

    for col in ["parent_high_edu", "east_background"]:
        if df[col].dtype == 'O' or df[col].dtype.name == 'category':
            df, new_dummies = create_dummy(df, col)
            df, new_dummies = sanitize_column_names(df, new_dummies)
            if col == "parent_high_edu":
                parent_high_edu_dummies = new_dummies
            elif col == "east_background":
                east_background_dummies = new_dummies
        else:
            df[col] = df[col].astype(int)
            if col == "parent_high_edu":
                parent_high_edu_dummies = [col]
            elif col == "east_background":
                east_background_dummies = [col]

    for col in sex_dummies + partner_dummies + migback_dummies + any_sibling_bafog_dummies + lives_at_home_dummies + parent_high_edu_dummies + east_background_dummies:
        df[col] = df[col].astype(int)

    vars = [
        "theoretical_bafög",
        "age",
        "plh0253",
        "plh0254",
        "plh0204_h",
    ] + sex_dummies + partner_dummies + migback_dummies + any_sibling_bafog_dummies + lives_at_home_dummies + parent_high_edu_dummies + east_background_dummies

    formula = "non_take_up ~ " + " + ".join(vars) + " - 1"

    lpm_results = run_linear_probability_model(df, formula, cluster_var="pid")

    config = load_project_config()
    model_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
    model_results_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(lpm_results, model_results_dir / "lpm_model.pkl")
    print(f"LPM model saved to {model_results_dir}")
