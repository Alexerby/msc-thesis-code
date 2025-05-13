import os
from pathlib import Path

from pipeline.build import BafoegPipeline
from loaders.registry import LoaderRegistry

from descriptives.plots import (plot_need_components_over_time, 
                                plot_reported_bafoeg_amounts_over_time)
from misc.utility_functions import load_project_config


def main():
    # Load config
    config = load_project_config()
    
    # Resolve save path
    figures_path = Path(os.path.expanduser(config["paths"]["results"]["figures"]))
    figures_path.mkdir(parents=True, exist_ok=True)
    need_components_path = figures_path / "need_components_over_time.png"
    reported_bafoeg_path = figures_path / "reported_bafoeg.png"

    # Initialize loader and pipeline
    loaders = LoaderRegistry()
    loaders.load_all()
    pipeline = BafoegPipeline(loaders)

    # Build tables and extract BAföG data
    tables = pipeline.build()
    bafoeg_df = tables["bafoeg_calculations"]

    # Generate and save figure
    plot_need_components_over_time(bafoeg_df, save_path=need_components_path)
    plot_reported_bafoeg_amounts_over_time(bafoeg_df, save_path=reported_bafoeg_path)


if __name__ == "__main__":
    main()
