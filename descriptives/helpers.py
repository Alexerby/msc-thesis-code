import os
import pandas as pd

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




def load_data(
    df_name: str,
    from_parquet: bool = False,
    parquet_dir: str = "~/Downloads/BAföG Results/parquets"
) -> pd.DataFrame:
    """
    Load student-level data either from a precomputed Parquet file or by building the full pipeline.

    Args:
        df_name (str): The name of the DataFrame to return.
        from_parquet (bool): If True, load from a Parquet file instead of rebuilding the pipeline.
        parquet_dir (str): Directory where Parquet files are stored.

    Returns:
        pd.DataFrame: The requested DataFrame.
    """
    if from_parquet:
        expanded_dir = os.path.expanduser(parquet_dir)
        path = os.path.join(expanded_dir, f"{df_name}.parquet")

        if not os.path.exists(path):
            available = os.listdir(expanded_dir)
            raise FileNotFoundError(
                f"Parquet file not found: {path}\nAvailable files in '{expanded_dir}':\n{available}"
            )

        return pd.read_parquet(path)
    else:
        loaders = LoaderRegistry()
        loaders.load_all()
        pipeline = BafoegPipeline(loaders)
        tables = pipeline.build()

        if df_name not in tables:
            raise KeyError(f"'{df_name}' not found in pipeline outputs. Available keys: {list(tables.keys())}")

        return tables[df_name]
