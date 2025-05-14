
import os
from pathlib import Path

from pipeline.build import BafoegPipeline
from misc.utility_functions import load_project_config
from loaders.registry import LoaderRegistry


def get_output_paths():
    """
    Load config and prepare output directories for saving income plots.
    Returns:
        dict with 'income_figures_dir', 'pdf_dir', and 'excess_income_dir' as Path objects.
    """
    config = load_project_config()
    figures_dir = Path(os.path.expanduser(config["paths"]["results"]["figures"]))
    income_figures_dir = figures_dir / "income"
    pdf_dir = income_figures_dir / "pdfs"
    excess_income_dir = income_figures_dir / "excess_income"

    income_figures_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    excess_income_dir.mkdir(parents=True, exist_ok=True)

    return {
        "income_figures_dir": income_figures_dir,
        "pdf_dir": pdf_dir,
        "excess_income_dir": excess_income_dir,
    }


def load_data(df_name: str):
    """
    Load student-level data using the BAföG pipeline.
    Returns:
        students_df (pd.DataFrame)
    """
    loaders = LoaderRegistry()
    loaders.load_all()
    pipeline = BafoegPipeline(loaders)
    tables = pipeline.build()
    return tables[df_name]
