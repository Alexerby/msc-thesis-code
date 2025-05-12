import os 
import json
from typing import Dict, Literal
from pathlib import Path
import pandas as pd
import re
import time
from functools import wraps



def timeit(method):
    @wraps(method)
    def timed(*args, **kwargs):
        start = time.time()
        result = method(*args, **kwargs)
        elapsed = time.time() - start
        print(f"⏱️ '{method.__name__}' executed in {elapsed:.2f} seconds.")
        return result
    return timed

def _norm(col: str) -> str:
    """Remove whitespace and make lowercase – for robust column matching."""
    return re.sub(r"\s+", "", col).lower()

def _auto_map(cols, patterns):
    """
    Build {old_name: new_name} where `patterns` is
    {new_name: list_of_regexes_on_normalised_column}.
    """
    rename = {}
    ncols = { _norm(c): c for c in cols }      # map normalised -> real
    for new, regexes in patterns.items():
        for rgx in regexes:
            # find first column whose *normalised* name matches pattern
            matches = [ real for norm, real in ncols.items()
                        if re.fullmatch(rgx, norm) ]
            if matches:
                rename[matches[0]] = new
                break          # stop after first match
    return rename


def get_config_path(filename: Path) -> Path:
    """
    Returns the absolute path to the config file,
    assuming it is located in a 'config' folder one level above this file.
    """
    this_dir = Path(os.path.abspath(__file__)).parent
    parent_dir = this_dir.parent
    path = parent_dir / "config" / filename
    return path

def load_config(config_path: Path) -> Dict:
    """
    Load a JSON config.

    :param config_path: Path to the JSON configuration file
    :return: Dictionary of the config
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
        return config


def load_project_config(filename: str = "config.json") -> Dict:
    """
    Loads the main project configuration file located in ../config/.

    :param filename: Name of the config file (default is 'config.json')
    :return: Dictionary with loaded config
    """
    config_path = get_config_path(filename)
    return load_config(config_path)

# def export_data(
#     data_type: Literal["csv", "excel"],
#     df: pd.DataFrame,
#     output_filename: str,
#     sheet_name: str = "Sheet1"
# ):
#     """Exports data to the results directory without overwriting existing files.
#     
#     Args:
#         data_type (str): "csv" or "excel"
#         df (pd.DataFrame): The DataFrame to export
#         output_filename (str): Base name for the output file (no extension needed)
#         sheet_name (str): Optional sheet name for Excel exports
#     """
#
#     # if df is None: # Unreachable, comment out for now
#     #     raise ValueError("Dataframe is empty")
#
#     config_path = get_config_path(Path("config.json"))
#     config = load_config(config_path)
#     results_folder = Path(config["paths"]["results"]["dataframes"])
#
#     # Ensure output directory exists
#     results_folder = results_folder.expanduser().resolve()
#     results_folder.mkdir(parents=True, exist_ok=True)
#
#     # Append correct extension
#     ext = ".csv" if data_type == "csv" else ".xlsx"
#     output_filename = f"{output_filename}{ext}" if not output_filename.endswith(ext) else output_filename
#
#     # Avoid overwrite
#     file_path = results_folder / output_filename
#     file_base, file_extension = os.path.splitext(output_filename)
#     counter = 1
#     while file_path.exists():
#         file_path = results_folder / f"{file_base} ({counter}){file_extension}"
#         counter += 1
#
#     # Export
#     if data_type == "csv":
#         df.to_csv(file_path, index=False)
#     elif data_type == "excel":
#         df.to_excel(file_path, index=False, sheet_name=sheet_name)


def export_parquet(df: pd.DataFrame, base_name: str, results_key: str = "dataframes") -> Path:
    """
    Export a DataFrame as a .parquet file to the configured results directory,
    avoiding overwriting by appending (1), (2), etc.
    """
    path = _next_available_path(base_name, ".parquet", results_key)
    df.to_parquet(path, index=False)
    print(f"✅ Parquet file written → {path}")
    return path

def _next_available_path(base_filename: str, ext: str, results_key: str) -> Path:
    """
    Return a Path inside the configured results directory that doesn't overwrite
    an existing file by appending ' (1)', ' (2)', ... if needed.
    """
    config_path = get_config_path(Path("config.json"))
    config = load_config(config_path)
    folder = Path(config["paths"]["results"][results_key]).expanduser().resolve()
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"{base_filename}{ext}" if not base_filename.endswith(ext) else base_filename
    file_path = folder / filename
    base, ext_only = os.path.splitext(filename)
    counter = 1
    while file_path.exists():
        file_path = folder / f"{base} ({counter}){ext_only}"
        counter += 1
    return file_path


@timeit
def export_tables(
        tables: dict[str, pd.DataFrame],
        base_name: str = "bafoeg_results",
        results_key: str = "dataframes"
):
    """
    Save each DataFrame in `tables` to its own sheet in one workbook,
    using the configured results directory and avoiding overwrite.
    """
    path = _next_available_path(base_name, ".xlsx", results_key)
    with pd.ExcelWriter(path, engine="xlsxwriter") as xl:
        for sheet, frame in tables.items():
            frame.to_excel(xl, sheet_name=sheet[:31], index=False)  # Excel max sheet name = 31 chars
    print(f"✅ Excel workbook written → {path}")




