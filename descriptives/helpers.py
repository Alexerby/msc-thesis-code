import os
import pandas as pd

from pathlib import Path

from pipeline.build import BafoegPipeline
from misc.utility_functions import load_project_config
from loaders.registry import LoaderRegistry


def get_output_paths(which: str = None):
    """
    Load config and prepare output directories.
    Optionally return just one subdirectory if `which` is specified.

    Parameters:
        which (str): One of ['income_figures_dir', 'pdf_dir', 'excess_income_dir']

    Returns:
        dict or Path: Either the full dict or just the requested Path
    """
    config = load_project_config()
    figures_dir = Path(os.path.expanduser(config["paths"]["results"]["figures"]))
    income_figures_dir = figures_dir / "income"
    pdf_dir = income_figures_dir / "pdfs"
    excess_income_dir = income_figures_dir / "excess_income"

    for d in [income_figures_dir, pdf_dir, excess_income_dir]:
        d.mkdir(parents=True, exist_ok=True)

    paths = {
        "income_figures_dir": income_figures_dir,
        "pdf_dir": pdf_dir,
        "excess_income_dir": excess_income_dir,
    }

    if which:
        if which not in paths:
            raise ValueError(f"Invalid output path key: {which}")
        return paths[which]
    return paths



def load_data(
    df_name: str,
    from_parquet: bool = False,
    parquet_dir: str = "~/Documents/MScEcon/Semester 2/Master Thesis I/Microsimulation/parquets"
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
