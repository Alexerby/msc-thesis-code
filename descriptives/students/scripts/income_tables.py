import os
from pathlib import Path

import pandas as pd

from misc.utility_functions import load_project_config
from loaders.registry import LoaderRegistry
from pipeline.build import BafoegPipeline

from ..tables.income import summarize_gross_income


def get_output_path() -> Path:
    """
    Get path to save the income summary table.
    """
    config = load_project_config()
    tables_dir = Path(os.path.expanduser(config["paths"]["results"]["tables"]))
    tables_dir.mkdir(parents=True, exist_ok=True)
    return tables_dir / "income_summary.csv"


def load_student_data() -> pd.DataFrame:
    """
    Load student-level data using the BAföG pipeline.
    """
    loaders = LoaderRegistry()
    loaders.load_all()
    pipeline = BafoegPipeline(loaders)
    tables = pipeline.build()
    return tables["students"]


def run_income_summary(save: bool = True, print_preview: bool = False):
    """
    Generate and optionally save the gross income summary table.
    """
    df = load_student_data()
    summary = summarize_gross_income(df, print_table=True)

    if print_preview:
        print(summary.head())

    if save:
        output_path = get_output_path()
        summary.to_csv(output_path, index=False)
        print(f"Saved income summary table to: {output_path}")


def main():
    run_income_summary()


if __name__ == "__main__":
    main()
