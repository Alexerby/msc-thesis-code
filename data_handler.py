from typing import List, Union, Dict, Tuple, cast
from pathlib import Path

import pandas as pd

from misc.utility_functions import get_config_path, load_config


def resolve_dataset_path(
    file: Union[str, Path],
    config_section: str
) -> Tuple[Path, Path]:
    """
    Resolves the full file path for a dataset given its name and config section.
    """
    config_path = get_config_path(Path("config.json"))
    config: Dict = load_config(config_path)

    data_dir = Path(config["paths"]["data"][config_section]).expanduser().resolve()

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    file = Path(file).with_suffix(".csv")
    file_path = data_dir / file

    if not file_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {file_path}")

    return data_dir, file_path



class DatasetLoader:
    def __init__(self, file: Union[str, Path], config_section: str) -> None:
        self.data_dir, self.file_path = resolve_dataset_path(file, config_section)
        self.file = Path(file).with_suffix(".csv")
        self.data: pd.DataFrame = pd.DataFrame()

    @property
    def dataset_name(self) -> str:
        return self.file.stem

    def load_dataset(self, columns, chunk_size: int = 10000, filetype: str = "csv"):
        """
        Load a dataset file in chunks from CSV.
        """
        file_path = self.data_dir / self.file
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")

        if filetype == "csv":
            chunks = pd.read_csv(file_path, chunksize=chunk_size, usecols=columns)
            self.data = pd.concat(chunks, ignore_index=True)
            print("CSV loading complete.")
        else:
            raise ValueError(f"Unsupported file type: {filetype}")

    def load_first_n_rows(self, columns: List[str], n: int = 10000):
        """Load the first n rows of a dataset file."""
        self.data = pd.read_csv(self.file_path, nrows=n, usecols=columns)

    def filter_data(self, variables: Union[str, List[str]], filter_values: List):
        self.data = cast(pd.DataFrame, self.data)

        if isinstance(variables, str):
            variables = [variables]

        for var in variables:
            if var not in self.data.columns:
                raise ValueError(f"Column '{var}' not found in data.")
            before = len(self.data)
            self.data = self.data.loc[~self.data[var].isin(filter_values)].copy()
            after = len(self.data)
            print(f"Filtered '{var}': {before - after} rows removed, {after} remaining.")


class SOEPStatutoryInputs(DatasetLoader):
    def __init__(self, file: Union[str, Path]):
        super().__init__(file, config_section="self_composed")


class SOEPDataHandler(DatasetLoader):
    #TODO: 
    # fix error handling for cached version
    # currently when new variable is added 
    # it fails the cache version. 
    # overwrite the cache version to handle this

    def __init__(self, file: Union[str, Path]):
        super().__init__(file, config_section="soep")

    def get_cache_path(self) -> Path:
        config_path = get_config_path(Path("config.json"))
        config: Dict = load_config(config_path)
        cache_dir = Path(config["paths"]["data"]["soep_cached"]).expanduser().resolve()
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / self.file.with_suffix(".parquet")

    def load_dataset(self, columns, chunk_size: int = 10000, filetype: str = "csv", use_cache: bool = True):
        file_path = self.data_dir / self.file
        parquet_path = self.get_cache_path()

        if use_cache and parquet_path.exists():
            try:
                print(f"✅ Loading cached Parquet: {parquet_path}")
                self.data = pd.read_parquet(parquet_path, columns=columns)
                return
            except (ValueError, OSError, pd.errors.ParserError, ImportError, Exception) as e:
                print(f"⚠️ Failed to load Parquet with requested columns: {e}")
                print("🔁 Falling back to CSV and overwriting cache.")

        # Load CSV if no cache or if Parquet failed
        print(f"📄 Loading CSV: {file_path}")
        chunks = pd.read_csv(file_path, chunksize=chunk_size, usecols=columns)
        self.data = pd.concat(chunks, ignore_index=True)
        print("✅ CSV loading complete.")

        # Cache new Parquet version
        if use_cache:
            print(f"💾 Caching to Parquet: {parquet_path}")
            self.data.to_parquet(parquet_path, index=False)

    def _apply_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        mapping_file = self.data_dir / f"{self.dataset_name}_values.csv"
        df_mappings = pd.read_csv(mapping_file)
        mappings_by_variable = {
            var: dict(zip(group["value"].astype(str), group["label_de"]))
            for var, group in df_mappings.groupby("variable")
        }

        for column in df.columns:
            if column in mappings_by_variable and column not in ["pid", "cid", "hid", "syear"]:
                df[column] = df[column].astype(str)

        df = df.dropna(axis=1, how='all')
        return df
