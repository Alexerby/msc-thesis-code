import pandas as pd
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




def load_data(df_name: str, source: str = "pipeline", parquet_dir: str | Path = "~/Downloads/bafoeg_results") -> pd.DataFrame:
    """
    Load BAföG data either from the pipeline or from cached Parquet files.

    Parameters
    ----------
    df_name : str
        One of "students", "parents", "siblings", "bafoeg_calculations"
    source : str
        Either "pipeline" (default) or "parquet"
    parquet_dir : str or Path
        Path to directory with precomputed parquet files (default: ~/Downloads/bafoeg_results)

    Returns
    -------
    pd.DataFrame
    """
    if source == "pipeline":
        loaders = LoaderRegistry()
        loaders.load_all()
        pipeline = BafoegPipeline(loaders)
        tables = pipeline.build()
        return tables[df_name]

    elif source == "parquet":
        parquet_dir = Path(os.path.expanduser(parquet_dir))
        file_path = parquet_dir / f"{df_name}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"{file_path} not found. Run export first.")
        return pd.read_parquet(file_path)

    else:
        raise ValueError(f"Unsupported source '{source}'. Use 'pipeline' or 'parquet'.")

