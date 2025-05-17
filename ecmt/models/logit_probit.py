import pandas as pd
from statsmodels.formula.api import logit as sm_logit, probit as sm_probit
from typing import List, Dict, Optional

from ecmt.parquet_loader import BafoegParquetLoader


# === CONFIGURATION ===

DEFAULT_MISSING_CODES = [-1, -2, -3, -8, -9, -99]

DEFAULT_REGISTRY_MERGES = [
    {"key": "ppath", "columns": ["migback"]},
]

DEFAULT_EXTERNAL_MERGES = [
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/students.parquet",
        "columns": ["age", "east_background", "sex", "gross_monthly_income", "has_partner", "lives_at_home", "bula"],
        "left_on": ["pid", "syear"],
        "right_on": ["pid", "syear"]
    },
    {
        "parquet_path": "~/Downloads/BAföG Results/parquets/parents_joint.parquet",
        "columns": ["joint_income", "isced_mean"],
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

DEFAULT_CATEGORICALS = ["migback", "isced_mean",]


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

def make_dummies(df, categoricals):
    df = pd.get_dummies(df, columns=categoricals, drop_first=True)
    dummy_vars = []
    for cat in categoricals:
        dummy_vars += [col for col in df.columns if col.startswith(f"{cat}_")]
    return df, dummy_vars

def add_income_vars(df):
    df["parental_joint_income_k"] = df["joint_income"] / 1000
    df["gross_monthly_income_k"] = df["gross_monthly_income"] / 1000
    return df

def restrict_to_eligible(df, eligibility_col="theoretical_eligibility", received_col="received_bafög"):
    df = df[df[eligibility_col] == 1].copy() # Copy a df of only theoretically eligible students


    # Create new column from received col, making 0/1 switch place. 
    # -----------------------------------
    # received_col    non_take_up 
    #            0              1
    #            1              0
    #            0              1
    #            1              0
    # -----------------------------------
    df["non_take_up"] = (df[received_col] == 0).astype(int) 
    return df

# === MODELING ===

def run_binary_model(
    df: pd.DataFrame,
    formula: str,
    model_type: str = "logit",
    **fit_kwargs
):
    model_map = {"logit": sm_logit, "probit": sm_probit}
    if model_type not in model_map:
        raise ValueError(f"Unknown model_type '{model_type}'. Use 'logit' or 'probit'.")
    model = model_map[model_type](formula=formula, data=df)
    return model.fit(**fit_kwargs)

# === PIPELINE WRAPPER ===

def full_bafoeg_pipeline(
    base_parquet_path: str,
    registry_merges: Optional[List[Dict]] = None,
    external_merges: Optional[List[Dict]] = None,
    categoricals: Optional[List[str]] = None,
    missing_codes: Optional[List[int]] = None,
    Z_vars: Optional[List[str]] = None,
    B_vars: Optional[List[str]] = None
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

    categoricals = categoricals or DEFAULT_CATEGORICALS
    df = clean_and_impute_categoricals(df, categoricals, pipeline.missing_codes)
    df, D_vars = make_dummies(df, categoricals)

    # Variable groups (pass as args for flexibility)
    Z_vars = Z_vars or ["age", "parental_joint_income_k", "gross_monthly_income_k"]
    B_vars = B_vars or ["sex", "has_partner", "lives_at_home", "any_sibling_bafog", "east_background"]

    X_vars = Z_vars + B_vars + D_vars
    formula = "non_take_up ~ " + " + ".join(X_vars)

    print("\n=== Fitting LOGIT model ===")
    result = run_binary_model(df, formula=formula, model_type="logit")
    print(result.summary())

    print("\n=== Fitting PROBIT model ===")
    result_probit = run_binary_model(df, formula=formula, model_type="probit")
    print(result_probit.summary())
    return result, result_probit

# === USAGE ===

if __name__ == "__main__":
    full_bafoeg_pipeline(
        base_parquet_path="~/Downloads/BAföG Results/parquets/bafoeg_calculations.parquet",
        registry_merges=DEFAULT_REGISTRY_MERGES,
        external_merges=DEFAULT_EXTERNAL_MERGES,
        categoricals=DEFAULT_CATEGORICALS,
        missing_codes=DEFAULT_MISSING_CODES,
        # You can add or override variable lists as needed!
    )
