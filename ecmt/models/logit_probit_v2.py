from pathlib import Path
import pandas as pd
import numpy as np

from statsmodels.genmod.generalized_linear_model import GLM
from statsmodels.genmod.families import Binomial
from statsmodels.genmod.families.links import logit as logit_link, probit as probit_link
from statsmodels.api import OLS, add_constant

from typing import List, Dict, Optional
from ecmt.parquet_loader import BafoegParquetLoader
from ecmt.helpers import load_config
import joblib
import patsy

# === CONFIGURATION ===

DEFAULT_MISSING_CODES = [-1, -2, -3, -8, -9, -99]

DEFAULT_EXTERNAL_MERGES = [
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/students.parquet",
        "columns": ["age", "east_background", "sex", "gross_monthly_income",
                    "has_partner", "lives_at_home", "bula", "num_children",
                    "migback"],
        "left_on": ["pid", "syear"],
        "right_on": ["pid", "syear"],
    },
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/parents_joint.parquet",
        "columns": ["joint_income", "parent_high_edu"],
        "left_on": ["pid", "syear"],
        "right_on": ["student_pid", "syear"],
    },
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/siblings_joint.parquet",
        "columns": ["any_sibling_bafog"],
        "left_on": ["pid", "syear"],
        "right_on": ["student_pid", "syear"],
    },
]

MIGBACK_DUMMY = "has_migback"
DEFAULT_CATEGORICALS = []

# === DATA LOADER CLASS ===

class BafoegDataPipeline:
    def __init__(self, base_parquet_path, external_merges=None, missing_codes=None):
        self.loader = BafoegParquetLoader(base_parquet_path)
        self.external_merges = external_merges or []
        self.missing_codes = missing_codes or DEFAULT_MISSING_CODES

    def run_merges(self, validate_merge=True):
        for instruction in self.external_merges:
            parquet_paths = instruction["parquet_path"]
            if isinstance(parquet_paths, str):
                parquet_paths = [parquet_paths]
            for path in parquet_paths:
                df_ext = pd.read_parquet(path)
                left_on, right_on = instruction["left_on"], instruction["right_on"]
                columns_to_keep = list(set(right_on + instruction["columns"]))
                df_ext = df_ext[columns_to_keep].drop_duplicates(subset=right_on).copy()
                pre_merge_len = len(self.loader.df)
                self.loader.df = self.loader.df.merge(
                    df_ext, left_on=left_on, right_on=right_on, how=instruction.get("how", "left")
                )
                self.loader.merged_vars.extend(instruction["columns"])
                if validate_merge and len(self.loader.df) > pre_merge_len:
                    raise ValueError(f"Merge caused row inflation: {pre_merge_len} → {len(self.loader.df)}")
        self.loader.describe()

    def get_dataframe(self):
        return self.loader.get()

# === UTILITIES ===

def add_migback_dummy(df, missing_codes):
    df[MIGBACK_DUMMY] = (
        df["migback"].where(~df["migback"].isin(missing_codes)).isin([1, 2]).astype(int)
    )
    return df

def make_dummies(df, categoricals, drop_first=True):
    df = pd.get_dummies(df, columns=categoricals, drop_first=drop_first)
    dummy_vars = []
    for cat in categoricals:
        dummy_vars += [col for col in df.columns if col.startswith(f"{cat}_")]
    return df, dummy_vars

def add_income_vars(df):
    if "joint_income" in df.columns:
        df["joint_income"] = pd.to_numeric(df["joint_income"], errors='coerce')
        df["joint_income_log"] = np.where(df["joint_income"] > 0, np.log(df["joint_income"]), np.nan)
    if "gross_monthly_income" in df.columns:
        df["gross_monthly_income"] = pd.to_numeric(df["gross_monthly_income"], errors='coerce')
        df["gross_monthly_income_log"] = np.where(df["gross_monthly_income"] > 0, np.log(df["gross_monthly_income"]), np.nan)
    return df

def restrict_to_eligible(df, eligibility_col="theoretical_eligibility", received_col="received_bafög"):
    df = df[df[eligibility_col] == 1].copy()
    df["non_take_up"] = (df[received_col] == 0).astype(int)
    return df

# === MODELING ===


def run_binary_model(df, formula, model_type="logit", cluster_var=None, cov_type=None, weight_var=None, **fit_kwargs):
    y, X = patsy.dmatrices(formula, data=df, return_type="dataframe")

    needed_cols = list(y.columns) + list(X.columns)
    if cluster_var:
        needed_cols.append(cluster_var)
    if weight_var:
        needed_cols.append(weight_var)

    df_clean = df.dropna(subset=needed_cols).copy()

    y = y.loc[df_clean.index]
    X = X.loc[df_clean.index]

    weights = df_clean[weight_var] if weight_var else None
    groups = df_clean[cluster_var] if cluster_var else None

    link_func = {"logit": logit_link(), "probit": probit_link()}[model_type]
    model = GLM(y, X, family=Binomial(link=link_func), var_weights=weights)

    fit_args = {}
    if cluster_var:
        fit_args["cov_type"] = "cluster"
        fit_args["cov_kwds"] = {"groups": groups}
    elif cov_type:
        fit_args["cov_type"] = cov_type

    return model.fit(**fit_args, **fit_kwargs)


# === PIPELINE ===

def full_bafoeg_pipeline(
    base_parquet_path: str,
    Z_vars: List[str],
    B_vars: List[str],
    categoricals: List[str],
    external_merges: Optional[List[Dict]] = None,
    missing_codes: Optional[List[int]] = None,
    cluster_var: Optional[str] = None,
    cov_type: Optional[str] = None,
    weight_var: Optional[str] = None,
):
    pipeline = BafoegDataPipeline(base_parquet_path, external_merges, missing_codes)
    pipeline.run_merges()
    df = pipeline.get_dataframe()
    df = add_income_vars(df)
    df["age_sq"] = df["age"] ** 2
    df = restrict_to_eligible(df)
    df = add_migback_dummy(df, pipeline.missing_codes)
    D_vars = [MIGBACK_DUMMY]

    if categoricals:
        df, more_dummies = make_dummies(df, categoricals, drop_first=True)
        D_vars += more_dummies

    # === Model 1 & 2: Logit & Probit (direct, no IV) ===
    X_vars_direct = Z_vars + B_vars + D_vars
    formula_direct = "non_take_up ~ " + " + ".join(X_vars_direct) + " -1"

    print("\n=== [1] Standard LOGIT model ===")
    result_logit = run_binary_model(df, formula_direct, "logit", cluster_var, cov_type, weight_var)
    print(result_logit.summary())

    print("\n=== [2] Standard PROBIT model ===")
    result_probit = run_binary_model(df, formula_direct, "probit", cluster_var, cov_type, weight_var)
    print(result_probit.summary())

    # === First Stage for IV ===
    first_stage_model = None
    if "theoretical_bafög" in Z_vars and "total_base_need" in df.columns:
        print("\n=== First Stage: theoretical_bafög ~ total_base_need + controls ===")
        Z_vars_iv = [v for v in Z_vars if v != "theoretical_bafög"]
        first_stage_controls = Z_vars_iv + B_vars + D_vars + ["total_base_need"]
        df_fs = df.dropna(subset=["theoretical_bafög"] + first_stage_controls).copy()

        y_fs = df_fs["theoretical_bafög"]
        X_fs = add_constant(df_fs[first_stage_controls])
        first_stage_model = OLS(y_fs, X_fs).fit()
        print(first_stage_model.summary())
        print("First-stage R²:", round(first_stage_model.rsquared, 4))
        print("First-stage F-stat:", round(first_stage_model.fvalue, 2))

        df.loc[df_fs.index, "theoretical_bafög_hat"] = first_stage_model.fittedvalues
        df["theoretical_bafög_hat"] = df["theoretical_bafög_hat"].astype(float)

        # === Model 3 & 4: IV Logit & IV Probit ===
        X_vars_iv = Z_vars_iv + ["theoretical_bafög_hat"] + B_vars + D_vars
        formula_iv = "non_take_up ~ " + " + ".join(X_vars_iv) + " -1"

        print("\n=== [3] IV LOGIT model (fitted theoretical_bafög) ===")
        result_iv_logit = run_binary_model(df, formula_iv, "logit", cluster_var, cov_type, weight_var)
        print(result_iv_logit.summary())

        print("\n=== [4] IV PROBIT model (fitted theoretical_bafög) ===")
        result_iv_probit = run_binary_model(df, formula_iv, "probit", cluster_var, cov_type, weight_var)
        print(result_iv_probit.summary())

    else:
        result_iv_logit = None
        result_iv_probit = None

    return result_logit, result_probit, result_iv_logit, result_iv_probit, first_stage_model

# === EXECUTION ===

if __name__ == "__main__":
    result_logit, result_probit, result_iv_logit, result_iv_probit, first_stage_model = full_bafoeg_pipeline(
        base_parquet_path="~/Downloads/BAföG Results/parquets/bafoeg_calculations.parquet",
        external_merges=DEFAULT_EXTERNAL_MERGES,
        categoricals=[],
        missing_codes=DEFAULT_MISSING_CODES,
        # weight_var="phrf",
        cluster_var="pid",
        Z_vars=[
            "age", "age_sq", "joint_income_log", "theoretical_bafög",
            "gross_monthly_income_log", "plh0204_h", "plh0253", "plh0254",
        ],
        B_vars=[
            "sex", "has_partner", "lives_at_home", "any_sibling_bafog",
            "east_background", "parent_high_edu"
        ],
    )

    config = load_config()
    model_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
    model_results_dir.mkdir(parents=True, exist_ok=True)

    # Save all model results
    joblib.dump(result_logit, model_results_dir / "logit_model.pkl")
    joblib.dump(result_probit, model_results_dir / "probit_model.pkl")
    if result_iv_logit:
        joblib.dump(result_iv_logit, model_results_dir / "iv_logit_model.pkl")
    if result_iv_probit:
        joblib.dump(result_iv_probit, model_results_dir / "iv_probit_model.pkl")
    if first_stage_model:
        with open(model_results_dir / "first_stage_summary.txt", "w") as f:
            f.write(first_stage_model.summary().as_text())
