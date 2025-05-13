import os
from pathlib import Path

from matplotlib.pyplot import figure

from misc.utility_functions import load_project_config
from loaders.registry import LoaderRegistry
from pipeline.build import BafoegPipeline

from ..plots.income_plots import (
    plot_gross_income_over_time,
    plot_gross_income_pdfs_over_time,
)


def get_output_paths():
    """
    Load config and prepare output directories for saving income plots.
    Returns:
        dict with 'income_figures_dir' and 'pdf_dir' as Path objects.
    """
    config = load_project_config()
    figures_dir = Path(os.path.expanduser(config["paths"]["results"]["figures"]))
    income_figures_dir = figures_dir / "income"
    pdf_dir = income_figures_dir / "pdfs"

    income_figures_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    return {
        "income_figures_dir": income_figures_dir,
        "pdf_dir": pdf_dir
    }


def load_student_data():
    """
    Load student-level data using the BAföG pipeline.
    Returns:
        students_df (pd.DataFrame)
    """
    loaders = LoaderRegistry()
    loaders.load_all()
    pipeline = BafoegPipeline(loaders)
    tables = pipeline.build()
    return tables["students"]


def run_income_plots():
    """
    Execute all income-related plots and save them to disk.
    """
    paths = get_output_paths()
    students_df = load_student_data()

    plot_gross_income_over_time(
        students_df,
        save_path=paths["income_figures_dir"] / "gross_income_trend.png"
    )

    plot_gross_income_pdfs_over_time(
        students_df,
        save_path=paths["pdf_dir"]
    )


def main():
    run_income_plots()


if __name__ == "__main__":
    main()
