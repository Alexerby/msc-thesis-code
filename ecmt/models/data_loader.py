import pandas as pd
from pathlib import Path
from misc.utility_functions import load_project_config

config = load_project_config()
dir = config["paths"]["results"]["parquet_files"]

def load_and_merge(dir: str = dir):
    parquet_dir = Path(dir).expanduser()

    # Load all necessary parquet files
    bafoeg_df = pd.read_parquet(parquet_dir / "bafoeg_calculations.parquet")
    students_df = pd.read_parquet(parquet_dir / "students.parquet")
    siblings_joint_df = pd.read_parquet(parquet_dir / "siblings_joint.parquet")
    parents_joint_df = pd.read_parquet(parquet_dir / "parents_joint.parquet")

    # Select columns to keep from base
    base_cols = [
        "pid", 
        "syear", 
        "theoretical_bafög", 
        "received_bafög", 
        "theoretical_eligibility", 
        "excess_income_stu",
        "excess_income_par",
        "non_take_up_obs",
        "plh0253",
        "plh0254",
        "plh0204_h",
    ]
    base_sub = bafoeg_df[base_cols]

    # Students: columns to keep
    students_cols = [
        "pid", 
        "syear", 
        "age", 
        "sex", 
        "gross_monthly_income", 
        "has_partner", 
        "lives_at_home", 
        "num_children",
        "migback",
        "east_background"
    ]
    students_sub = students_df[students_cols]

    # Siblings joint: columns to keep
    siblings_joint_cols = ["student_pid", "syear", "any_sibling_bafog"]
    siblings_joint_sub = siblings_joint_df[siblings_joint_cols]

    # Parents joint: columns to keep
    parents_joint_cols = ["student_pid", "syear", "joint_income", "parent_high_edu"]
    parents_joint_sub = parents_joint_df[parents_joint_cols]

    # Merge base + students on pid and syear
    merged_df = base_sub.merge(
        students_sub,
        how="left",
        on=["pid", "syear"]
    )

    # Merge siblings joint on pid = student_pid and syear
    merged_df = merged_df.merge(
        siblings_joint_sub,
        how="left",
        left_on=["pid", "syear"],
        right_on=["student_pid", "syear"]
    )

    # Merge parents joint on pid = student_pid and syear
    merged_df = merged_df.merge(
        parents_joint_sub,
        how="left",
        left_on=["pid", "syear"],
        right_on=["student_pid", "syear"]
    )

    # Drop duplicate student_pid columns after merges if they exist
    merged_df.drop(columns=["student_pid_x", "student_pid_y", "student_pid"], errors='ignore', inplace=True)

    return merged_df
