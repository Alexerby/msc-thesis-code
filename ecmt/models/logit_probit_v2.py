from pathlib import Path
import pandas as pd
import numpy as np

from statsmodels.formula.api import logit as sm_logit, probit as sm_probit
from typing import List, Dict, Optional
from ecmt.parquet_loader import BafoegParquetLoader
from ecmt.helpers import load_config
import joblib

# === CONFIGURATION ===

DEFAULT_MISSING_CODES = [-1, -2, -3, -8, -9, -99]

DEFAULT_REGISTRY_MERGES = [
    {"key": "ppath", "columns": ["migback"]},
]

DEFAULT_EXTERNAL_MERGES = [
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/students.parquet",
        "columns": [
                    "age", 
                    "east_background", 
                    "sex", 
                    "gross_monthly_income", 
                    "has_partner", 
                    "lives_at_home", 
                    "bula",
                    "num_children"
                    ],
        "left_on": ["pid", "syear"],
        "right_on": ["pid", "syear"]
    },
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/parents_joint.parquet",
        "columns": ["joint_income", "parent_high_edu"],
        "left_on": ["pid", "syear"],
        "right_on": ["student_pid", "syear"]
    },    
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/siblings_joint.parquet",
        "columns": ["any_sibling_bafog"],
        "left_on": ["pid", "syear"],
        "right_on": ["student_pid", "syear"]
    },    
]

# MIGRATION BACKGROUND: NEW DUMMY NAME
MIGBACK_DUMMY = "has_migback"

DEFAULT_CATEGORICALS = []  # migback will be handled as dummy, not categorical

# === HUMAN-FRIENDLY LABEL MAPS ===

ISCED_LABELS = {
    0: "Parent: In school",
    1: "Parent: Primary education",
    2: "Parent: Lower secondary (reference)",  # typically dropped as reference
    3: "Parent: Upper secondary",
    4: "Parent: Post-secondary non-tertiary",
    5: "Parent: Short-cycle tertiary",
    6: "Parent: Bachelor or equivalent",
    7: "Parent: Master or equivalent",
    8: "Parent: Doctorate or equivalent"
}

MIGBACK_LABELS = {
    0: "No migration background (reference)",
    1: "Has migration background"
}

def get_dummy_label(col):
    """Return human-friendly label for dummy variable columns."""
    if col == MIGBACK_DUMMY:
        return "Migration background (direct or indirect)"
    if col.startswith("migback_"):
        code = int(col.split("_")[-1])
        return MIGBACK_LABELS.get(code, col)
    return col.replace("_", " ").capitalize()

# === DATA LOADER CLASS ===

class BafoegDataPipeline:
    def __init__(
        self,
        base_parquet_path: str,
        registry_merges: Optional[List[Dict]] = None,
        external_merges: Optional[List[Dict]] = None,
        missing_codes: Optional[List[int]] = None,
    ):
        self.loader = BafoegParquetLoader(base_parquet_path)
        self.registry_merges = registry_merges or []
        self.external_merges = external_merges or []
        self.missing_codes = missing_codes or DEFAULT_MISSING_CODES

    def run_merges(self, validate_merge: bool = True):
        # Registry merges
        for instruction in self.registry_merges:
            self.loader.merge_variable(
                key=instruction["key"],
                columns=instruction["columns"],
                on=instruction.get("on", ["pid", "syear"]),
                how=instruction.get("how", "left"),
                validate=validate_merge,
            )
        # External merges
        for instruction in self.external_merges:
            if "left_on" not in instruction or "right_on" not in instruction:
                raise ValueError(
                    f"External merge must specify both 'left_on' and 'right_on'. Got: {instruction}"
                )
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
                    df_ext,
                    left_on=left_on,
                    right_on=right_on,
                    how=instruction.get("how", "left"),
                )
                self.loader.merged_vars.extend(instruction["columns"])
                if validate_merge:
                    post_merge_len = len(self.loader.df)
                    if post_merge_len > pre_merge_len:
                        raise ValueError(
                            f"Merge caused row inflation: {pre_merge_len} → {post_merge_len}. "
                            f"Check merge keys: left_on={left_on}, right_on={right_on}"
                        )
        self.loader.describe()

    def get_dataframe(self):
        return self.loader.get()

# === FEATURE ENGINEERING UTILITIES ===

def add_migback_dummy(df, missing_codes):
    """Create a single binary dummy for migration background."""
    if "migback" not in df:
        raise ValueError("Column 'migback' not in DataFrame!")
    df[MIGBACK_DUMMY] = (
        df["migback"].where(~df["migback"].isin(missing_codes))
        .isin([1, 2])
        .astype(int)
    )
    return df

def clean_and_impute_categoricals(df, categoricals, missing_codes):
    """Clean, impute, and encode categorical columns."""
    for col in categoricals:
        print(f"\n--- Imputing {col} ---")
        if col not in df:
            raise ValueError(f"Column {col} not in DataFrame!")
        # Replace codes with NA
        df.loc[df[col].isin(missing_codes), col] = pd.NA
        df = df.sort_values(['pid', 'syear'])
        df[col] = df.groupby('pid')[col].ffill().bfill()
        # Int handling for clean dummies
        df[col] = df[col].astype('float').round().astype('Int64')
        df[col] = df[col].fillna(-99).astype(int)
        df[col] = df[col].astype(str)
    return df

def make_dummies(df, categoricals, drop_first=True):
    df = pd.get_dummies(df, columns=categoricals, drop_first=drop_first)
    dummy_vars = []
    for cat in categoricals:
        dummy_vars += [col for col in df.columns if col.startswith(f"{cat}_")]
    return df, dummy_vars



def add_income_vars(df):
    """
    Clean and log-transform income-related variables.
    Overwrites the original columns.
    """
    income_columns = [
        "joint_income",
        "gross_monthly_income",
    ]
    
    for col in income_columns:
        if col in df.columns:
            # Set non-positive and missing values to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')  # ensure numeric
            df[col] = df[col].where(df[col] > 0, np.nan)
            df[col] = np.log(df[col])
    
    return df

def restrict_to_eligible(df, eligibility_col="theoretical_eligibility", received_col="received_bafög"):
    df = df[df[eligibility_col] == 1].copy() # Copy a df of only theoretically eligible students
    df["non_take_up"] = (df[received_col] == 0).astype(int) 
    return df

# === MODELING ===

def run_binary_model(
    df: pd.DataFrame,
    formula: str,
    model_type: str = "logit",
    cluster_var: Optional[str] = None,
    cov_type: Optional[str] = None,
    weights=None,
    **fit_kwargs
):
    model_map = {"logit": sm_logit, "probit": sm_probit}
    if model_type not in model_map:
        raise ValueError(f"Unknown model_type '{model_type}'. Use 'logit' or 'probit'.")
    model = model_map[model_type](formula=formula, data=df)
    if cluster_var is not None:
        fit_kwargs['cov_type'] = 'cluster'
        fit_kwargs['cov_kwds'] = {'groups': df[cluster_var]}
    elif cov_type is not None:
        fit_kwargs['cov_type'] = cov_type
    # Pass weights if provided
    result = model.fit(weights=weights, **fit_kwargs)
    return result

def print_model_table(result, dummy_vars):
    """Print readable regression table for key dummies."""
    print("\nVariable\tCoef.\tStd.Err.\tp-value")
    for v in dummy_vars:
        label = get_dummy_label(v)
        try:
            coef = result.params[v]
            se = result.bse[v]
            pval = result.pvalues[v]
            print(f"{label}\t{coef:.3f}\t{se:.3f}\t{pval:.3f}")
        except KeyError:
            pass  # Could happen with collinearity or dropped levels

# === PIPELINE WRAPPER ===

def full_bafoeg_pipeline(
    base_parquet_path: str,
    registry_merges: Optional[List[Dict]] = None,
    external_merges: Optional[List[Dict]] = None,
    categoricals: Optional[List[str]] = None,
    missing_codes: Optional[List[int]] = None,
    Z_vars: Optional[List[str]] = None,
    B_vars: Optional[List[str]] = None,
    cluster_var: Optional[str] = None,
    cov_type: Optional[str] = None,
):
    pipeline = BafoegDataPipeline(
        base_parquet_path=base_parquet_path,
        registry_merges=registry_merges,
        external_merges=external_merges,
        missing_codes=missing_codes,
    )
    pipeline.run_merges()

    df = pipeline.get_dataframe()
    df = add_income_vars(df)
    df = restrict_to_eligible(df)

    # --- MIGBACK DUMMY ---
    df = add_migback_dummy(df, pipeline.missing_codes)
    D_vars = [MIGBACK_DUMMY]

    # If you have other categoricals, handle them as usual:
    categoricals = categoricals or DEFAULT_CATEGORICALS
    if categoricals:
        df = clean_and_impute_categoricals(df, categoricals, pipeline.missing_codes)
        df, more_dummies = make_dummies(df, categoricals, drop_first=True)
        D_vars += more_dummies

    # Handle missing codes for interaction vars (ensure no bad codes remain)
    for col in ["plh0253", "plh0254"]:
        if col in df.columns:
            df.loc[df[col].isin(DEFAULT_MISSING_CODES), col] = pd.NA

    # Variable groups (pass as args for flexibility)
    Z_vars = Z_vars or ["age", "joint_income", "gross_monthly_income", "theoretical_bafög"]
    B_vars = B_vars or ["sex", "has_partner", "lives_at_home", "any_sibling_bafog", "east_background"]

    # Remove plh0253 and plh0254 from X_vars if present, as they will be added via interaction in formula
    X_vars = [v for v in Z_vars + B_vars + D_vars if v not in ["plh0253", "plh0254"]]



    # Construct formula with main effects, interaction, and dummies
    formula = "non_take_up ~ " + " + ".join(X_vars)

    # === APPLY WEIGHTS ===
    if "phrf" not in df.columns:
        raise ValueError("Column 'phrf' not found in DataFrame! Is it in your base parquet?")
    df = df[df["phrf"].notna() & (df["phrf"] > 0)].copy()
    weights = df["phrf"]

    print("\n=== Fitting LOGIT model (with weights) ===")
    result = run_binary_model(
        df, 
        formula=formula, 
        model_type="logit", 
        cluster_var=cluster_var, 
        cov_type=cov_type,
        weights=weights
    )
    print(result.summary())
    print(f"McFadden's pseudo-R² (logit): {result.prsquared:.4f}")

    print_model_table(result, D_vars)
    marg_eff_logit = result.get_margeff(at='overall')
    print("\nLOGIT Average Marginal Effects (AMEs):")
    print(marg_eff_logit.summary())

    print("\n=== Fitting PROBIT model (with weights) ===")
    result_probit = run_binary_model(
        df, 
        formula=formula, 
        model_type="probit", 
        cluster_var=cluster_var, 
        cov_type=cov_type,
        weights=weights
    )
    print(result_probit.summary())
    print(f"McFadden's pseudo-R² (probit): {result_probit.prsquared:.4f}")

    print_model_table(result_probit, D_vars)
    marg_eff_probit = result_probit.get_margeff(at='overall')
    print("\nPROBIT Average Marginal Effects (AMEs):")
    print(marg_eff_probit.summary())

    print(df.shape)

    return result, result_probit



def run_probit_year_by_year(df, formula, year_col='syear', cluster_var=None, weights=None):
    results = {}
    years = sorted(df[year_col].dropna().unique())
    
    for year in years:
        print(f"\n--- Running probit for year {year} ---")
        df_year = df[df[year_col] == year].copy()
        
        if df_year.empty:
            print(f"No data for year {year}, skipping.")
            continue
        
        try:
            result = run_binary_model(
                df_year, formula, model_type="probit", cluster_var=cluster_var, weights=weights
            )
            results[year] = result
            
            # Print key info for dummies, assuming you want that
            # Get dummy vars from the formula or pass as argument (for example)
            # Here you could parse formula or just pass dummy_vars explicitly
            dummy_vars = [term for term in result.params.index if term != 'Intercept']
            print_model_table(result, dummy_vars)
        except Exception as e:
            print(f"Error fitting model for year {year}: {e}")
    
    return results

# === USAGE ===
if __name__ == "__main__":
    result, result_probit = full_bafoeg_pipeline(
        base_parquet_path="~/Downloads/BAföG Results/parquets/bafoeg_calculations.parquet",
        registry_merges=DEFAULT_REGISTRY_MERGES,
        external_merges=DEFAULT_EXTERNAL_MERGES,
        categoricals = [],  # No categoricals, migback now handled as dummy
        missing_codes=DEFAULT_MISSING_CODES,
        cov_type="HC2",
        Z_vars = ["age", 
                  "joint_income", 
                  "gross_monthly_income", 
                  "theoretical_bafög", 
                  "plh0204_h",
                  ],
        B_vars = ["sex", 
                  "has_partner", 
                  "lives_at_home", 
                  "any_sibling_bafog", 
                  "east_background", 
                  "parent_high_edu",
                  ], 
        )



    # === EXPORT THE MODELS ===
    config = load_config()
    model_results_dir = Path(config["paths"]["results"]["model_results"]).expanduser()
    model_results_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(result, model_results_dir / "logit_model.pkl")
    joblib.dump(result_probit, model_results_dir / "probit_model.pkl")
