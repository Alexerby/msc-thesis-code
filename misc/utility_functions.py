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
        results_key: str = "dataframes",
        output_path: str | None = None
):
    """
    Save each DataFrame in `tables` to its own sheet in one Excel workbook.
    If `output_path` is given, save directly there. Otherwise, generate a path
    using the configured results directory and avoid overwriting existing files.
    """
    if output_path is None:
        path = _next_available_path(base_name, ".xlsx", results_key)
    else:
        path = output_path

    with pd.ExcelWriter(path, engine="xlsxwriter") as xl:
        for sheet, frame in tables.items():
            frame.to_excel(xl, sheet_name=sheet[:31], index=False)  # Excel max sheet name = 31 chars
    print(f"✅ Excel workbook written → {path}")

