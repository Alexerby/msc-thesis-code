import pandas as pd
from descriptives.helpers import load_data
from tabulate import tabulate

from .conditional_types.household import non_take_up_by_hgtyp_per_year
from .conditional_types.employment_status import non_take_up_by_pgemplst
from .conditional_types.no_children import non_take_up_by_num_children_per_year
from .conditional_types.migback import non_take_up_by_migback_per_year
from .conditional_types.sex import non_take_up_by_sex_per_year
from .conditional_types.bula import non_take_up_by_bula_per_year
from .conditional_types.east import non_take_up_by_east_per_year
from .conditional_types.any_sibling_bafog import non_take_up_by_any_sibling_bafog_per_year
from .conditional_types.parents_high_edu import non_take_up_by_parent_high_edu_per_year





def compute_conditional_probs_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute conditional probabilities Pr(R=r | M=m) for all combinations (r,m) in {0,1}²,
    grouped by survey year, expressed as percentages (0–100).
    """
    df = df.copy()
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)
    df = df.dropna(subset=["syear"])

    results = []

    for syear, group in df.groupby("syear"):
        row = {"syear": syear}
        for m in [0, 1]:
            g = group[group["M"] == m]
            total = len(g)
            for r in [0, 1]:
                key = f"P(R={r} | M={m})"
                prob = 100 * (g["R"] == r).mean() if total > 0 else pd.NA
                row[key] = prob
        results.append(row)

    return pd.DataFrame(results).sort_values("syear")


def compute_overall_conditional_probs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute overall conditional probabilities Pr(R=r | M=m) for all (r,m) ∈ {0,1}²
    without grouping by year. Expressed as percentages (0–100).
    
    Returns:
        A single-row DataFrame with columns:
            - P(R=0 | M=0), P(R=1 | M=0), P(R=0 | M=1), P(R=1 | M=1)
    """
    df = df.copy()
    df["M"] = df["theoretical_eligibility"].fillna(0).astype(int)
    df["R"] = df["received_bafög"].fillna(0).astype(int)

    results = {}
    for m in [0, 1]:
        g = df[df["M"] == m]
        total = len(g)
        for r in [0, 1]:
            key = f"P(R={r} | M={m})"
            prob = 100 * (g["R"] == r).mean() if total > 0 else pd.NA
            results[key] = prob

    return pd.DataFrame([results])




def main():
    # Datasets 
    bc_df = load_data("bafoeg_calculations", from_parquet=True)
    stu_df = load_data("students", from_parquet=True)
    sib_joint_df = load_data("siblings_joint", from_parquet=True)
    siblings_df = sib_joint_df[["student_pid", "syear", "any_sibling_bafog"]].drop_duplicates()

    parents_joint_df = load_data("parents_joint", from_parquet=True)

    # Merge on pid <-> student_pid and syear
    bc_df = bc_df.merge(
        siblings_df,
        left_on=["pid", "syear"],
        right_on=["student_pid", "syear"],
        how="left"
    )


    bc_df = bc_df.merge(
        parents_joint_df,
        left_on=["pid", "syear"],
        right_on=["student_pid", "syear"],
        how="left"
    )

    # Merge
    main_df = bc_df.merge(stu_df, on=["pid", "syear"], how="left")

    cond_probs_by_year = compute_conditional_probs_by_year(main_df)
    cond_probs = compute_overall_conditional_probs(main_df)
    print(tabulate(cond_probs_by_year, headers="keys", tablefmt="github", floatfmt=".3f"))
    print(tabulate(cond_probs, headers="keys", tablefmt="github", floatfmt=".3f"))



if __name__ == "__main__":
    main()
